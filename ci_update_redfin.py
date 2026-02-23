"""Lightweight CI script to update Redfin estimates in docs/data.json.

Bypasses the database entirely — reads properties.csv for URLs,
scrapes Redfin pages with requests + BeautifulSoup, and patches
docs/data.json in place.
"""
from __future__ import annotations

import csv
import json
import os
import random
import re
import sys
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

# --- User-Agent pool (copied from config.py to avoid importing heavy deps) ---
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROPERTIES_CSV = os.path.join(BASE_DIR, "properties.csv")
DATA_JSON = os.path.join(BASE_DIR, "docs", "data.json")


# --- Scraping helpers (copied from scraper.py to avoid undetected_chromedriver import) ---

def _get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def _fetch_page(url):
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        print(f"  FETCH ERROR: {e}")
        return None


def _parse_price(text):
    if not text:
        return None
    text = text.strip().replace(",", "").replace("$", "")
    match = re.search(r"(\d+\.?\d*)\s*[Kk]", text)
    if match:
        return float(match.group(1)) * 1000
    match = re.search(r"(\d+\.?\d*)\s*[Mm]", text)
    if match:
        return float(match.group(1)) * 1_000_000
    match = re.search(r"(\d+\.?\d*)", text)
    if match:
        return float(match.group(1))
    return None


def scrape_redfin(url):
    soup = _fetch_page(url)
    if not soup:
        return None

    estimate_pattern = re.compile(r"Redfin Estimate[^$]*\$([0-9,]+)", re.IGNORECASE)
    match = estimate_pattern.search(soup.get_text())
    if match:
        return _parse_price(match.group(1))

    for selector in [
        'div[data-rf-test-id="avmLdpPrice"]',
        'span[class*="EstimatePrice"]',
    ]:
        el = soup.select_one(selector)
        if el:
            price = _parse_price(el.get_text())
            if price:
                return price

    return None


# --- Main logic ---

def load_properties():
    """Read properties.csv and return list of dicts with unit_number and redfin_url."""
    props = []
    with open(PROPERTIES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("redfin_url", "").strip()
            if url:
                props.append({
                    "unit": int(row["unit_number"]),
                    "address": row["address"],
                    "redfin_url": url,
                })
    return props


def load_data_json():
    with open(DATA_JSON) as f:
        return json.load(f)


def save_data_json(data):
    with open(DATA_JSON, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def main():
    props = load_properties()
    print(f"Loaded {len(props)} properties with Redfin URLs")

    data = load_data_json()
    # Build lookup by unit number
    prop_map = {p["unit"]: p for p in data["properties"]}

    today = date.today().isoformat()
    successes = 0
    failures = 0

    for i, prop in enumerate(props):
        unit = prop["unit"]
        print(f"[{i+1}/{len(props)}] Unit {unit}: {prop['redfin_url']}")

        price = scrape_redfin(prop["redfin_url"])
        if price:
            print(f"  -> ${price:,.0f}")
            if unit in prop_map:
                prop_map[unit]["redfin"] = price
                prop_map[unit]["estimate_date"] = today
            successes += 1
        else:
            print("  -> FAILED")
            failures += 1

        # Rate limit between requests (skip after last)
        if i < len(props) - 1:
            time.sleep(random.uniform(1.0, 2.0))

    # Update exported_at timestamp
    data["exported_at"] = f"{today}T00:00:00Z"

    save_data_json(data)

    print(f"\nDone: {successes} successes, {failures} failures out of {len(props)}")

    # Exit with error if more than half failed (likely blocked)
    if len(props) > 0 and failures > len(props) / 2:
        print("ERROR: >50% of scrapes failed — likely blocked by Redfin")
        sys.exit(1)


if __name__ == "__main__":
    main()
