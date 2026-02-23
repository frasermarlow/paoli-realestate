#!/usr/bin/env python3
"""Populate Zillow and Redfin URLs for all properties."""

from __future__ import annotations

import csv
import logging
import random
import re
import time

import requests

import config

logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def build_zillow_url(address: str) -> str:
    """Construct a Zillow URL from an address."""
    # "111 Woodgate Ln Paoli PA 19301" -> "111-Woodgate-Ln-Paoli-PA-19301"
    slug = address.replace(",", "").replace("  ", " ").strip().replace(" ", "-")
    return f"https://www.zillow.com/homes/{slug}_rb/"


def build_redfin_url(address: str) -> str:
    """Construct a Redfin URL from an address.

    Pattern: https://www.redfin.com/PA/Paoli/111-Woodgate-Ln-19301/home/XXXXXXX
    Without the home ID, Redfin may still resolve the address slug.
    """
    # "111 Woodgate Ln Paoli PA 19301" -> parts
    parts = address.replace(",", "").split()
    # Extract state, city, zip
    # Assumes format: "NUM Street St City ST ZIP"
    zip_code = parts[-1]
    state = parts[-2]
    city = parts[-3]
    street_parts = parts[:-3]  # everything before city
    street_slug = "-".join(street_parts)
    return f"https://www.redfin.com/{state}/{city}/{street_slug}-{zip_code}/home/"


def main():
    # Read current CSV
    rows = []
    with open(config.PROPERTIES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    zillow_count = 0
    redfin_count = 0

    for i, row in enumerate(rows):
        address = row["address"]
        logger.info("[%d/%d] Processing %s", i + 1, total, address)

        # Zillow — deterministic URL pattern
        if not row.get("zillow_url"):
            row["zillow_url"] = build_zillow_url(address)
            zillow_count += 1

        # Redfin — deterministic URL pattern
        if not row.get("redfin_url"):
            row["redfin_url"] = build_redfin_url(address)
            redfin_count += 1

    # Write updated CSV
    with open(config.PROPERTIES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Updated {config.PROPERTIES_CSV}")
    print(f"  Zillow URLs added: {zillow_count}")
    print(f"  Redfin URLs found: {redfin_count}/{total}")
    print(f"\nRun 'python3 main.py init' to load updated URLs into the database.")


if __name__ == "__main__":
    main()
