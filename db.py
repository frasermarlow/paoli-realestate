from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

import config

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True)
    address = Column(String, nullable=False, unique=True)
    unit_number = Column(String)
    zillow_url = Column(String)
    redfin_url = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    estimates = relationship("Estimate", back_populates="property", order_by="Estimate.captured_at")
    sales = relationship("Sale", back_populates="property", order_by="Sale.sale_date")

    def __repr__(self):
        return f"<Property {self.unit_number}: {self.address}>"


class Estimate(Base):
    __tablename__ = "estimates"

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    source = Column(String, nullable=False)  # "zillow" or "redfin"
    estimated_price = Column(Float, nullable=False)
    captured_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    property = relationship("Property", back_populates="estimates")

    def __repr__(self):
        return f"<Estimate {self.source}: ${self.estimated_price:,.0f} @ {self.captured_at}>"


class Sale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    asking_price = Column(Float)
    sale_price = Column(Float, nullable=False)
    sale_date = Column(DateTime, nullable=False)
    recorded_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    property = relationship("Property", back_populates="sales")

    def __repr__(self):
        return f"<Sale ${self.sale_price:,.0f} on {self.sale_date}>"


engine = create_engine(config.DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    """Create all tables."""
    Base.metadata.create_all(engine)
    logger.info("Database initialized at %s", config.DB_PATH)


def get_session() -> Session:
    return SessionLocal()


def get_or_create_property(session: Session, address: str, unit_number: str = None,
                           zillow_url: str = None, redfin_url: str = None) -> Property:
    prop = session.query(Property).filter_by(address=address).first()
    if prop:
        updated = False
        if zillow_url and prop.zillow_url != zillow_url:
            prop.zillow_url = zillow_url
            updated = True
        if redfin_url and prop.redfin_url != redfin_url:
            prop.redfin_url = redfin_url
            updated = True
        if updated:
            session.commit()
        return prop
    prop = Property(
        address=address,
        unit_number=unit_number,
        zillow_url=zillow_url,
        redfin_url=redfin_url,
    )
    session.add(prop)
    session.commit()
    logger.info("Created property: %s", prop)
    return prop


def add_estimate(session: Session, property_id: int, source: str, estimated_price: float) -> Estimate:
    estimate = Estimate(
        property_id=property_id,
        source=source,
        estimated_price=estimated_price,
    )
    session.add(estimate)
    session.commit()
    logger.info("Added %s estimate for property %d: $%.0f", source, property_id, estimated_price)
    return estimate


def add_sale(session: Session, property_id: int, sale_price: float,
             sale_date: datetime, asking_price: float = None) -> Sale:
    sale = Sale(
        property_id=property_id,
        sale_price=sale_price,
        sale_date=sale_date,
        asking_price=asking_price,
    )
    session.add(sale)
    session.commit()
    logger.info("Added sale for property %d: $%.0f on %s", property_id, sale_price, sale_date)
    return sale


def get_all_properties(session: Session) -> list[Property]:
    return session.query(Property).order_by(Property.unit_number).all()


def get_estimates_for_property(session: Session, property_id: int) -> list[Estimate]:
    return (
        session.query(Estimate)
        .filter_by(property_id=property_id)
        .order_by(Estimate.captured_at)
        .all()
    )


def get_sales_with_estimates(session: Session) -> list[dict]:
    """Get all sales paired with the most recent estimate before each sale."""
    results = []
    sales = session.query(Sale).all()
    for sale in sales:
        for source in ("zillow", "redfin"):
            estimate = (
                session.query(Estimate)
                .filter(
                    Estimate.property_id == sale.property_id,
                    Estimate.source == source,
                    Estimate.captured_at <= sale.sale_date,
                )
                .order_by(Estimate.captured_at.desc())
                .first()
            )
            if estimate:
                error = estimate.estimated_price - sale.sale_price
                pct_error = (error / sale.sale_price) * 100
                results.append({
                    "property_id": sale.property_id,
                    "address": sale.property.address,
                    "source": source,
                    "estimated_price": estimate.estimated_price,
                    "sale_price": sale.sale_price,
                    "sale_date": sale.sale_date,
                    "estimate_date": estimate.captured_at,
                    "error": error,
                    "pct_error": pct_error,
                })
    return results


def seed_db(csv_path: str = None):
    """Load properties from CSV into the database."""
    csv_path = csv_path or config.PROPERTIES_CSV
    session = get_session()
    count = 0
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                get_or_create_property(
                    session,
                    address=row["address"].strip(),
                    unit_number=row.get("unit_number", "").strip() or None,
                    zillow_url=row.get("zillow_url", "").strip() or None,
                    redfin_url=row.get("redfin_url", "").strip() or None,
                )
                count += 1
        logger.info("Seeded %d properties from %s", count, csv_path)
    finally:
        session.close()
    return count
