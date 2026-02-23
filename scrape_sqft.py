"""Scrape square footage from Redfin for all 60 Woodgate units."""
from __future__ import annotations

import csv
import json
import os
import random
import re
import time

import requests
from bs4 import BeautifulSoup

import config

SQFT_PATH = os.path.join(config.DATA_DIR, "sqft.json")


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def scrape_sqft(url: str) -> int | None:
    """Extract square footage from a Redfin listing page."""
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Primary: data-rf-test-id="abp-sqFt"
    el = soup.select_one('[data-rf-test-id="abp-sqFt"]')
    if el:
        text = el.get_text().replace(",", "")
        match = re.search(r"(\d+)", text)
        if match:
            return int(match.group(1))

    # Fallback: class containing sqft-section
    el = soup.select_one(".sqft-section")
    if el:
        text = el.get_text().replace(",", "")
        match = re.search(r"(\d+)", text)
        if match:
            return int(match.group(1))

    # Last resort: regex
    text = soup.get_text()
    match = re.search(r"([\d,]+)\s*sq\s*ft", text, re.IGNORECASE)
    if match:
        return int(match.group(1).replace(",", ""))

    return None


def main():
    # Load existing results to allow resuming
    if os.path.exists(SQFT_PATH):
        with open(SQFT_PATH) as f:
            results = json.load(f)
        print(f"Loaded {len(results)} existing sqft records")
    else:
        results = {}

    # Read properties.csv
    units = []
    with open(config.PROPERTIES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            units.append((row["unit_number"], row["redfin_url"]))

    total = len(units)
    skipped = 0
    scraped = 0
    failed = 0

    for i, (unit, url) in enumerate(units):
        if unit in results:
            skipped += 1
            continue

        print(f"[{i+1}/{total}] Unit {unit}...", end=" ")
        sqft = scrape_sqft(url)
        if sqft:
            results[unit] = sqft
            scraped += 1
            print(f"{sqft} sq ft")
        else:
            failed += 1
            print("FAILED")

        # Save after each successful scrape
        with open(SQFT_PATH, "w") as f:
            json.dump(results, f, indent=2)

        # Rate limit
        if i < total - 1:
            time.sleep(random.uniform(config.MIN_DELAY, config.MAX_DELAY))

    print(f"\nDone: {scraped} scraped, {skipped} skipped, {failed} failed")
    print(f"Total sqft records: {len(results)}")


if __name__ == "__main__":
    main()
