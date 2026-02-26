"""Lightweight CI script to update Redfin estimates in docs/data.json.

Bypasses the database entirely — reads properties.csv for URLs,
fetches Redfin estimates via their internal stingray API (with HTML
scraping as fallback), and patches docs/data.json in place.
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


# --- Scraping helpers ---

def _get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def _get_api_headers(referer_url):
    """Headers for Redfin stingray API requests."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer_url,
        "Connection": "keep-alive",
    }


def _extract_property_id(redfin_url):
    """Extract the numeric property ID from a Redfin URL like .../home/38879483."""
    match = re.search(r"/home/(\d+)", redfin_url)
    return match.group(1) if match else None


def _parse_api_response(text):
    """Parse Redfin stingray API response (prefixed with '{}&&')."""
    prefix = "{}&&"
    if text.startswith(prefix):
        text = text[len(prefix):]
    return json.loads(text)


def _scrape_redfin_api(redfin_url):
    """Try to get the Redfin estimate via the internal stingray API."""
    property_id = _extract_property_id(redfin_url)
    if not property_id:
        print("  API: Could not extract property ID from URL")
        return None

    api_url = (
        f"https://www.redfin.com/stingray/api/home/details/avm"
        f"?propertyId={property_id}&accessLevel=1"
    )

    try:
        resp = requests.get(
            api_url,
            headers=_get_api_headers(redfin_url),
            timeout=15,
        )
        resp.raise_for_status()
        data = _parse_api_response(resp.text)

        # Navigate the response to find the estimate value
        payload = data.get("payload", {})

        # Try predictedValue (most common location)
        predicted = payload.get("predictedValue")
        if predicted:
            return float(predicted)

        # Try nested avm structure
        avm = payload.get("avm", {})
        for key in ("value", "amount", "predictedValue"):
            val = avm.get(key)
            if val:
                return float(val)

        # Try sectionPreviewText which sometimes has the price
        preview = payload.get("sectionPreviewText", "")
        if preview:
            price_match = re.search(r"\$([0-9,]+)", preview)
            if price_match:
                return float(price_match.group(1).replace(",", ""))

        print(f"  API: Got response but couldn't find estimate in payload")
        return None

    except requests.RequestException as e:
        print(f"  API ERROR: {e}")
        return None
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        print(f"  API PARSE ERROR: {e}")
        return None


def _fetch_page(url):
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        print(f"  HTML FETCH ERROR: {e}")
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


def _scrape_redfin_html(url):
    """Fallback: scrape the Redfin estimate from the HTML page."""
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


def scrape_redfin(url):
    """Get Redfin estimate: try API first, fall back to HTML scraping.

    Returns (price, method) where method is 'api', 'html', or None.
    """
    # Try the stingray API first (works better from cloud/CI environments)
    price = _scrape_redfin_api(url)
    if price:
        print("  (via API)")
        return price, "api"

    # Fall back to HTML scraping
    price = _scrape_redfin_html(url)
    if price:
        print("  (via HTML)")
        return price, "html"

    return None, None


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
    api_successes = 0
    html_successes = 0

    for i, prop in enumerate(props):
        unit = prop["unit"]
        print(f"[{i+1}/{len(props)}] Unit {unit}: {prop['redfin_url']}")

        price, method = scrape_redfin(prop["redfin_url"])
        if price:
            print(f"  -> ${price:,.0f}")
            if unit in prop_map:
                prop_map[unit]["redfin"] = price
                prop_map[unit]["estimate_date"] = today
            successes += 1
            if method == "api":
                api_successes += 1
            else:
                html_successes += 1
        else:
            print("  -> FAILED")
            failures += 1

        # Rate limit between requests (skip after last)
        if i < len(props) - 1:
            time.sleep(random.uniform(1.0, 2.0))

    # Compute stats for changelog
    redfin_vals = [p["redfin"] for p in prop_map.values() if p.get("redfin")]
    avg_redfin = round(sum(redfin_vals) / len(redfin_vals)) if redfin_vals else 0

    # Count new sales since last changelog entry
    changelog = data.get("changelog", [])
    prev_sales_count = changelog[-1]["total_sales"] if changelog else len(data.get("sales", []))
    current_sales_count = len(data.get("sales", []))
    new_sales = current_sales_count - prev_sales_count

    # Append changelog entry
    entry = {
        "date": today,
        "type": "redfin_update",
        "redfin_updated": successes,
        "redfin_failed": failures,
        "avg_redfin": avg_redfin,
        "new_sales": max(new_sales, 0),
        "total_sales": current_sales_count,
    }
    changelog.append(entry)
    data["changelog"] = changelog

    # Update exported_at timestamp
    data["exported_at"] = f"{today}T00:00:00Z"

    save_data_json(data)

    print(f"\nDone: {successes} successes ({api_successes} API, {html_successes} HTML), "
          f"{failures} failures out of {len(props)}")

    # Exit with error if more than half failed (likely blocked)
    if len(props) > 0 and failures > len(props) / 2:
        print("ERROR: >50% of scrapes failed — likely blocked by Redfin")
        sys.exit(1)


if __name__ == "__main__":
    main()
