"""Export Woodgate price data to JSON for the GitHub Pages dashboard."""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone

from sqlalchemy import desc

import config
from db import Estimate, Property, SessionLocal

DOCS_DIR = os.path.join(config.BASE_DIR, "docs")
OUTPUT_PATH = os.path.join(DOCS_DIR, "data.json")
SALES_CSV = os.path.join(config.BASE_DIR, "hoa_sales.csv")
SCRAPED_SALES = os.path.join(config.BASE_DIR, "data", "scraped_sales_history.json")
TAX_HISTORY = os.path.join(config.BASE_DIR, "data", "tax_history.json")
SQFT_PATH = os.path.join(config.BASE_DIR, "data", "sqft.json")


def load_hoa_sales():
    """Parse hoa_sales.csv and return SALE records with source tag."""
    sales = []
    with open(SALES_CSV, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0].strip('"') != "SALE":
                continue
            unit = int(row[1].strip('"'))
            date_str = row[2].strip('"')
            price = int(row[3].strip('"'))
            sales.append({
                "unit": unit,
                "date": date_str,
                "price": price,
                "source": "hoa",
            })
    return sales


def load_redfin_sales():
    """Load scraped Redfin sales history."""
    if not os.path.exists(SCRAPED_SALES):
        return []
    with open(SCRAPED_SALES) as f:
        data = json.load(f)
    redfin = data.get("redfin", {})
    sales = []
    for unit_str, records in redfin.items():
        unit = int(unit_str)
        for rec in records:
            sales.append({
                "unit": unit,
                "date": rec["date"],
                "price": rec["price"],
                "source": "redfin",
            })
    return sales


def is_duplicate(a, b, max_days=90):
    """Check if two sales records for the same unit are likely the same sale."""
    if a["unit"] != b["unit"]:
        return False
    # Exact duplicate
    if a["date"] == b["date"] and a["price"] == b["price"]:
        return True
    # Same price within max_days = likely duplicate (listed vs closed date)
    price_diff = abs(a["price"] - b["price"])
    pct_diff = price_diff / max(a["price"], 1) * 100
    if price_diff > 1000 and pct_diff > 1.0:
        return False
    try:
        da = datetime.strptime(a["date"], "%Y-%m-%d")
        db = datetime.strptime(b["date"], "%Y-%m-%d")
        return abs((da - db).days) <= max_days
    except ValueError:
        return False


def merge_sales(hoa_sales, redfin_sales):
    """Merge HOA and Redfin sales, deduplicating. HOA records take priority."""
    # Start with all HOA records
    merged = list(hoa_sales)

    # Add Redfin records that aren't duplicates of existing entries
    for r_sale in redfin_sales:
        is_dup = any(is_duplicate(r_sale, existing) for existing in merged)
        if not is_dup:
            merged.append(r_sale)

    merged.sort(key=lambda s: (s["date"], s["unit"]))
    return merged


def export_estimates():
    """Query DB for the latest Zillow and Redfin estimate per property."""
    session = SessionLocal()
    properties = []
    try:
        for prop in session.query(Property).order_by(Property.unit_number).all():
            entry = {
                "unit": int(prop.unit_number),
                "address": prop.address,
                "zillow": None,
                "redfin": None,
                "estimate_date": None,
                "zillow_date": None,
            }
            for source in ("zillow", "redfin"):
                est = (
                    session.query(Estimate)
                    .filter_by(property_id=prop.id, source=source)
                    .order_by(desc(Estimate.captured_at))
                    .first()
                )
                if est:
                    entry[source] = int(est.estimated_price)
                    est_date = est.captured_at.strftime("%Y-%m-%d")
                    if source == "zillow":
                        entry["zillow_date"] = est_date
                    if entry["estimate_date"] is None or est_date > entry["estimate_date"]:
                        entry["estimate_date"] = est_date
            properties.append(entry)
    finally:
        session.close()
    return properties


def load_tax_history():
    """Load scraped tax history data."""
    if not os.path.exists(TAX_HISTORY):
        return {}
    with open(TAX_HISTORY) as f:
        return json.load(f)


def load_sqft():
    """Load scraped square footage data."""
    if not os.path.exists(SQFT_PATH):
        return {}
    with open(SQFT_PATH) as f:
        return json.load(f)


def main():
    os.makedirs(DOCS_DIR, exist_ok=True)

    hoa_sales = load_hoa_sales()
    redfin_sales = load_redfin_sales()
    merged = merge_sales(hoa_sales, redfin_sales)
    taxes = load_tax_history()
    sqft = load_sqft()

    properties = export_estimates()
    # Merge sqft into property records
    for prop in properties:
        unit_key = str(prop["unit"])
        prop["sqft"] = sqft.get(unit_key)

    # Detect units needing Zillow data collection: units with a sale
    # more recent than their latest Zillow estimate.
    zillow_alerts = []
    for prop in properties:
        unit = prop["unit"]
        zillow_date = prop.get("zillow_date")
        # Find the most recent sale for this unit
        unit_sales = [s for s in merged if s["unit"] == unit]
        if not unit_sales:
            continue
        latest_sale = max(unit_sales, key=lambda s: s["date"])
        if not zillow_date or latest_sale["date"] > zillow_date:
            zillow_alerts.append({
                "unit": unit,
                "sale_date": latest_sale["date"],
                "sale_price": latest_sale["price"],
                "zillow_date": zillow_date,
            })

    # Preserve existing changelog from data.json (maintained by ci_update_redfin.py)
    existing_changelog = []
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH) as f:
            existing_changelog = json.load(f).get("changelog", [])

    data = {
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "properties": properties,
        "sales": merged,
        "taxes": taxes,
        "zillow_alerts": zillow_alerts,
        "changelog": existing_changelog,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    hoa_count = sum(1 for s in merged if s["source"] == "hoa")
    redfin_count = sum(1 for s in merged if s["source"] == "redfin")
    print(f"Exported {len(data['properties'])} properties, "
          f"{len(merged)} sales ({hoa_count} HOA + {redfin_count} Redfin) "
          f"to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
