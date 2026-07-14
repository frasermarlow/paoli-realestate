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

# --- Pacing to avoid tripping Redfin's anti-bot blocking ---
REQUEST_DELAY_RANGE = (2.0, 5.0)     # jitter between individual requests
BATCH_SIZE = 15                       # properties per batch
BATCH_PAUSE_RANGE = (30.0, 60.0)      # jitter pause between batches
BACKOFF_FAILURE_THRESHOLD = 3         # consecutive failures before backing off
BACKOFF_BASE_SECONDS = 45.0           # base pause, doubles each time it re-triggers
BACKOFF_MAX_SECONDS = 300.0
ABORT_AFTER_CONSECUTIVE_FAILURES = 12  # give up rather than backoff-loop for hours


def _pace(i, total, consecutive_failures, backoff_level):
    """Sleep between requests: per-request jitter, longer batch pauses, and
    exponential backoff if we appear to be getting blocked.

    Returns the (possibly updated) backoff_level.
    """
    if i >= total - 1:
        return backoff_level

    if consecutive_failures >= BACKOFF_FAILURE_THRESHOLD:
        pause = min(BACKOFF_BASE_SECONDS * (2 ** backoff_level), BACKOFF_MAX_SECONDS)
        pause += random.uniform(0, 10)
        print(f"  ...{consecutive_failures} failures in a row, backing off {pause:.0f}s")
        time.sleep(pause)
        return backoff_level + 1

    if (i + 1) % BATCH_SIZE == 0:
        pause = random.uniform(*BATCH_PAUSE_RANGE)
        print(f"  ...batch of {BATCH_SIZE} done, pausing {pause:.0f}s")
        time.sleep(pause)
        return 0

    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
    return backoff_level


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


def _scrape_redfin_sales(url):
    """Scrape recent sale history from a Redfin property page.

    Returns a list of {"date": "YYYY-MM-DD", "price": int} dicts for Sold events,
    or None if the page could not be fetched (distinct from "fetched fine, no sales").
    """
    from datetime import datetime as _dt

    soup = _fetch_page(url)
    if not soup:
        return None

    sales = []

    # Approach 1: structured table rows (various Redfin layouts)
    for row in soup.select(".BasicTable__row, [data-rf-test-id='sale-history'] tr, tr"):
        cells = row.select("td, th")
        row_text = " ".join(c.get_text(" ", strip=True) for c in cells)
        if "sold" not in row_text.lower():
            continue
        date_match = re.search(r"(\w{3}\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})", row_text)
        price_match = re.search(r"\$([0-9,]+)", row_text)
        if not (date_match and price_match):
            continue
        raw_date = date_match.group(1)
        for fmt in ("%b %d, %Y", "%m/%d/%Y"):
            try:
                sale_date = _dt.strptime(raw_date, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                sale_date = None
        if sale_date:
            price = int(price_match.group(1).replace(",", ""))
            if price > 10000:
                sales.append({"date": sale_date, "price": price})

    if sales:
        return sales

    # Approach 2: regex over full page text
    text = soup.get_text(" ")
    for m in re.finditer(
        r"(\d{1,2}/\d{1,2}/\d{4})\s+Sold\s+\$([0-9,]+)",
        text,
        re.IGNORECASE,
    ):
        try:
            sale_date = _dt.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
            price = int(m.group(2).replace(",", ""))
            if price > 10000:
                sales.append({"date": sale_date, "price": price})
        except ValueError:
            pass

    return sales


def detect_new_sales(props, existing_sales):
    """Scrape Redfin sale history for each property and return sales not in existing_sales.

    existing_sales: list of {"unit": int, "date": str, "price": int, ...}
    Returns list of new {"unit": int, "date": str, "price": int, "source": "redfin"}.
    """
    # Build a set of (unit, date, price) for fast lookup; also allow price within $1000
    known = {}
    for s in existing_sales:
        known.setdefault(s["unit"], []).append(s)

    def _is_known(unit, date, price):
        for s in known.get(unit, []):
            if s["date"] == date and abs(s["price"] - price) <= 1000:
                return True
        return False

    new_sales = []
    consecutive_failures = 0
    backoff_level = 0
    for i, prop in enumerate(props):
        unit = prop["unit"]
        print(f"  [sales check {i+1}/{len(props)}] Unit {unit}")
        scraped = _scrape_redfin_sales(prop["redfin_url"])
        if scraped is None:
            consecutive_failures += 1
            scraped = []
        else:
            consecutive_failures = 0
        for s in scraped:
            if not _is_known(unit, s["date"], s["price"]):
                print(f"    NEW SALE: Unit {unit} on {s['date']} for ${s['price']:,}")
                new_sales.append({
                    "unit": unit,
                    "date": s["date"],
                    "price": s["price"],
                    "source": "redfin",
                })

        if consecutive_failures >= ABORT_AFTER_CONSECUTIVE_FAILURES:
            remaining = len(props) - (i + 1)
            print(
                f"  ABORTING sales check: {consecutive_failures} consecutive failures — "
                f"skipping remaining {remaining} propert{'y' if remaining == 1 else 'ies'}."
            )
            break

        backoff_level = _pace(i, len(props), consecutive_failures, backoff_level)

    return new_sales


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

    consecutive_failures = 0
    backoff_level = 0
    blocked = False
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
            consecutive_failures = 0
            if method == "api":
                api_successes += 1
            else:
                html_successes += 1
        else:
            print("  -> FAILED")
            failures += 1
            consecutive_failures += 1

        if consecutive_failures >= ABORT_AFTER_CONSECUTIVE_FAILURES:
            remaining = len(props) - (i + 1)
            print(
                f"ABORTING: {consecutive_failures} consecutive failures — "
                f"Redfin appears fully blocked. Skipping remaining {remaining} propert"
                f"{'y' if remaining == 1 else 'ies'} and the sales check this run."
            )
            failures += remaining
            blocked = True
            break

        backoff_level = _pace(i, len(props), consecutive_failures, backoff_level)

    # Check Redfin property pages for new sales not already in data.json
    # (skip if we already gave up above — Redfin is blocking us either way)
    if blocked:
        discovered = []
    else:
        print("\nChecking for new sales on Redfin...")
        discovered = detect_new_sales(props, data.get("sales", []))
        if discovered:
            print(f"Found {len(discovered)} new sale(s) — adding to data.json")
            data["sales"] = data.get("sales", []) + discovered
            data["sales"].sort(key=lambda s: (s["date"], s["unit"]))
        else:
            print("No new sales detected.")

    # Compute stats for changelog
    redfin_vals = [p["redfin"] for p in prop_map.values() if p.get("redfin")]
    avg_redfin = round(sum(redfin_vals) / len(redfin_vals)) if redfin_vals else 0

    changelog = data.get("changelog", [])
    current_sales_count = len(data.get("sales", []))

    # Append changelog entry
    entry = {
        "date": today,
        "type": "redfin_update",
        "redfin_updated": successes,
        "redfin_failed": failures,
        "avg_redfin": avg_redfin,
        "new_sales": len(discovered),
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
