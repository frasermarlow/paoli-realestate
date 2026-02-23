import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import func

import config
from db import Estimate, Property, SessionLocal, get_all_properties
from scraper import scrape_batch

logger = logging.getLogger(__name__)


def get_next_batch(batch_size: int = None) -> list[Property]:
    """Select the next batch of properties to scrape, prioritizing those least recently scraped."""
    batch_size = batch_size or config.BATCH_SIZE
    session = SessionLocal()

    try:
        # Subquery: most recent estimate date per property
        latest_estimate = (
            session.query(
                Estimate.property_id,
                func.max(Estimate.captured_at).label("last_scraped"),
            )
            .group_by(Estimate.property_id)
            .subquery()
        )

        # Get properties ordered by last scraped (nulls first = never scraped)
        properties = (
            session.query(Property)
            .outerjoin(latest_estimate, Property.id == latest_estimate.c.property_id)
            .filter(
                # Only include properties that have at least one URL configured
                (Property.zillow_url.isnot(None)) | (Property.redfin_url.isnot(None))
            )
            .order_by(latest_estimate.c.last_scraped.asc().nullsfirst())
            .limit(batch_size)
            .all()
        )

        logger.info("Selected %d properties for next batch", len(properties))
        return properties
    finally:
        session.close()


def daily_scrape_job():
    """Job that runs daily to scrape the next batch of properties."""
    logger.info("Starting daily scrape job")
    properties = get_next_batch()
    if not properties:
        logger.warning("No properties with URLs configured. Skipping.")
        return
    results = scrape_batch(properties)
    successes = sum(1 for r in results if r.success)
    logger.info("Daily job complete: scraped %d properties, %d successful estimates", len(properties), successes)


def start_scheduler():
    """Start the APScheduler with the daily scrape job."""
    scheduler = BlockingScheduler()
    scheduler.add_job(
        daily_scrape_job,
        "cron",
        hour=config.SCRAPE_HOUR,
        minute=config.SCRAPE_MINUTE,
        id="daily_scrape",
        name="Daily property estimate scraper",
        misfire_grace_time=3600,
    )
    logger.info(
        "Scheduler started. Daily scrape at %02d:%02d, batch size %d",
        config.SCRAPE_HOUR,
        config.SCRAPE_MINUTE,
        config.BATCH_SIZE,
    )
    print(f"Scheduler running. Daily scrape at {config.SCRAPE_HOUR:02d}:{config.SCRAPE_MINUTE:02d}.")
    print("Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
