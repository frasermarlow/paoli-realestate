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
from datetime import date, timedelta

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
# Observed behavior across five separate runs: every run gets exactly ~6
# consecutive successes before 403s start, and once blocked, a fresh success
# unlocks after ~5.5-6 minutes regardless of backoff length. That looks like
# a burst allowance of ~6 requests on a rolling window of a few minutes, not
# an hourly quota. Testing staying under both: bursts of 5, paused 10 minutes
# apart (comfortably under the burst size and over the refill window).
REQUEST_DELAY_RANGE = (2.0, 5.0)     # jitter between individual requests within a burst
BURST_SIZE = 5                        # requests per burst
BURST_PAUSE_SECONDS = 600.0           # fixed pause between bursts (10 minutes)
MAX_UNITS_PER_RUN = 20                # cap per run so units age out in staggered waves instead of all at once
ABORT_AFTER_CONSECUTIVE_FAILURES = 15  # let a full run's queue complete for clean data, rather than give up early
FRESHNESS_DAYS = 30                   # every unit must have a successful Redfin check within this window
SALE_DATE_TOLERANCE_DAYS = 180        # same unit + price within this many days = same sale (HOA dates can be ~2 months off Redfin)


def _pace(i, total):
    """Sleep between requests: per-request jitter within a burst, and a
    fixed pause between bursts of BURST_SIZE requests."""
    if i >= total - 1:
        return

    if (i + 1) % BURST_SIZE == 0:
        print(f"  ...burst of {BURST_SIZE} done, pausing {BURST_PAUSE_SECONDS:.0f}s")
        time.sleep(BURST_PAUSE_SECONDS)
    else:
        time.sleep(random.uniform(*REQUEST_DELAY_RANGE))


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


# Links to Woodgate homes appear in page HTML both as hrefs and JSON-escaped
# (\u002F for "/") inside embedded data, e.g. nearby-homes and comparables.
_WOODGATE_LINK_RE = re.compile(
    r"/PA/Paoli/(\d+)-Woodgate-Ln-19301(?:/unit-[\w-]+)?/home/(\d+)"
)


def _find_woodgate_links(soup):
    """Return {unit_number: {redfin_property_id: path}} for every Woodgate home linked from a page."""
    html = str(soup).replace("\\u002F", "/").replace("\\/", "/")
    links = {}
    for m in _WOODGATE_LINK_RE.finditer(html):
        links.setdefault(int(m.group(1)), {}).setdefault(m.group(2), m.group(0))
    return links


def _scrape_redfin_sales(url, links_out=None):
    """Scrape recent sale history from a Redfin property page.

    Returns a list of {"date": "YYYY-MM-DD", "price": int} dicts for Sold events,
    or None if the page could not be fetched (distinct from "fetched fine, no sales").
    If links_out is a dict, Woodgate property links found on the page are merged into it.
    """
    from datetime import datetime as _dt

    soup = _fetch_page(url)
    if not soup:
        return None

    if links_out is not None:
        for unit, ids in _find_woodgate_links(soup).items():
            for pid, path in ids.items():
                links_out.setdefault(unit, {}).setdefault(pid, path)

    sales = []

    # Approach 1: structured table rows (various Redfin layouts). Redfin's
    # current sale history is div-based (.BasicTable__col), not <td>/<th>.
    for row in soup.select(".BasicTable__row, [data-rf-test-id='sale-history'] tr, tr"):
        cells = row.select(".BasicTable__col, td, th")
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
        r"(\d{1,2}/\d{1,2}/\d{4}|[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s+Sold\s+\$([0-9,]+)",
        text,
    ):
        for fmt in ("%m/%d/%Y", "%b %d, %Y"):
            try:
                sale_date = _dt.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                sale_date = None
        price = int(m.group(2).replace(",", ""))
        if sale_date and price > 10000:
            sales.append({"date": sale_date, "price": price})

    return sales


def detect_new_sales(props, existing_sales, links_out=None):
    """Scrape Redfin sale history for each property and return sales not in existing_sales.

    Checks each unit's redfin_url and, when present, its redfin_listing_url (the
    MLS listing record, which shows a sale ~1-3 weeks before public records do).

    existing_sales: list of {"unit": int, "date": str, "price": int, ...}
    Returns list of new {"unit": int, "date": str, "price": int, "source": "redfin"}.
    """
    # Match on unit + price within $1000 + date within SALE_DATE_TOLERANCE_DAYS.
    # The same sale shows different dates by source: MLS close date, public-records
    # recording date (often 1-3 weeks later), and HOA-reported date.
    known = {}
    for s in existing_sales:
        known.setdefault(s["unit"], []).append(s)

    def _is_known(unit, sale_date, price):
        d = date.fromisoformat(sale_date)
        for s in known.get(unit, []):
            if (abs(s["price"] - price) <= 1000
                    and abs((date.fromisoformat(s["date"]) - d).days) <= SALE_DATE_TOLERANCE_DAYS):
                return True
        return False

    # One entry per page fetch, so pacing counts requests rather than units.
    # Listing page first so a sale is recorded with the MLS close date rather
    # than the later public-records date.
    pages = []
    for prop in props:
        if prop.get("redfin_listing_url"):
            pages.append((prop["unit"], prop["redfin_listing_url"]))
        pages.append((prop["unit"], prop["redfin_url"]))

    new_sales = []
    consecutive_failures = 0
    for i, (unit, url) in enumerate(pages):
        print(f"  [sales check {i+1}/{len(pages)}] Unit {unit}: {url}")
        scraped = _scrape_redfin_sales(url, links_out)
        if scraped is None:
            consecutive_failures += 1
            scraped = []
        else:
            consecutive_failures = 0
        for s in scraped:
            if not _is_known(unit, s["date"], s["price"]):
                print(f"    NEW SALE: Unit {unit} on {s['date']} for ${s['price']:,}")
                sale = {
                    "unit": unit,
                    "date": s["date"],
                    "price": s["price"],
                    "source": "redfin",
                }
                new_sales.append(sale)
                # So the same sale on the unit's other page (different date) isn't added twice
                known.setdefault(unit, []).append(sale)

        if consecutive_failures >= ABORT_AFTER_CONSECUTIVE_FAILURES:
            remaining = len(pages) - (i + 1)
            print(
                f"  ABORTING sales check: {consecutive_failures} consecutive failures — "
                f"skipping remaining {remaining} page{'' if remaining == 1 else 's'}."
            )
            break

        _pace(i, len(pages))

    return new_sales


# --- Main logic ---

def load_properties():
    """Read properties.csv and return list of dicts with unit_number, redfin_url
    and (optional) redfin_listing_url."""
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
                    "redfin_listing_url": (row.get("redfin_listing_url") or "").strip(),
                })
    return props


def save_discovered_listing_urls(props, links):
    """Fill in redfin_listing_url in properties.csv for units that lack one.

    links: {unit: {redfin_property_id: path}} harvested from pages during the run.
    A unit's listing record is a second Redfin property ID for the same address;
    it's only recorded when exactly one such ID was seen. Returns units updated.
    """
    by_unit = {p["unit"]: p for p in props}
    found = {}
    for unit, ids in links.items():
        prop = by_unit.get(unit)
        if not prop or prop["redfin_listing_url"]:
            continue
        own_id = _extract_property_id(prop["redfin_url"])
        others = {pid: path for pid, path in ids.items() if pid != own_id}
        if len(others) == 1:
            found[unit] = "https://www.redfin.com" + next(iter(others.values()))
        elif len(others) > 1:
            print(f"  Unit {unit}: multiple candidate listing records {sorted(others)} — skipping")

    if not found:
        return []

    with open(PROPERTIES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)
    if "redfin_listing_url" not in fieldnames:
        fieldnames.append("redfin_listing_url")
    for row in rows:
        url = found.get(int(row["unit_number"]))
        if url and not (row.get("redfin_listing_url") or "").strip():
            row["redfin_listing_url"] = url
    with open(PROPERTIES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    for unit in sorted(found):
        print(f"  Unit {unit}: recorded listing URL {found[unit]}")
    return sorted(found)


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

    # Queue any unit that hasn't had a successful Redfin check within the
    # freshness window (or has never succeeded at all), oldest first, and
    # process the whole queue — not a fixed count. A bad week just grows
    # next week's queue instead of stranding a unit past the 30-day target.
    cutoff = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()

    def _estimate_date(prop):
        return prop_map.get(prop["unit"], {}).get("estimate_date")

    stale_props = sorted(
        (p for p in props if not _estimate_date(p) or _estimate_date(p) < cutoff),
        key=lambda p: _estimate_date(p) or "0000-00-00",
    )

    if not stale_props:
        print(f"All {len(props)} properties have a Redfin check within the last "
              f"{FRESHNESS_DAYS} days — nothing to do this run.")
        return

    if len(stale_props) > MAX_UNITS_PER_RUN:
        print(f"{len(stale_props)} stale; capping this run at the {MAX_UNITS_PER_RUN} oldest "
              f"(the rest roll over to next week).")
        stale_props = stale_props[:MAX_UNITS_PER_RUN]

    print(
        f"{len(stale_props)} of {len(props)} properties are stale "
        f"(no successful Redfin check in {FRESHNESS_DAYS}+ days): "
        f"{', '.join(str(p['unit']) for p in stale_props)}"
    )

    today = date.today().isoformat()
    successes = 0
    failures = 0
    api_successes = 0
    html_successes = 0

    consecutive_failures = 0
    blocked = False
    for i, prop in enumerate(stale_props):
        unit = prop["unit"]
        print(f"[{i+1}/{len(stale_props)}] Unit {unit}: {prop['redfin_url']}")

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
            remaining = len(stale_props) - (i + 1)
            print(
                f"ABORTING: {consecutive_failures} consecutive failures — "
                f"Redfin appears fully blocked. Skipping remaining {remaining} propert"
                f"{'y' if remaining == 1 else 'ies'} and the sales check this run."
            )
            failures += remaining
            blocked = True
            break

        _pace(i, len(stale_props))

    # Check Redfin property pages for new sales not already in data.json
    # (skip if we already gave up above — Redfin is blocking us either way)
    if blocked:
        discovered = []
    else:
        print("\nChecking for new sales on Redfin...")
        links = {}
        discovered = detect_new_sales(stale_props, data.get("sales", []), links)
        save_discovered_listing_urls(props, links)
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
          f"{failures} failures out of {len(stale_props)} stale "
          f"({len(props)} properties total)")

    # Exit with error if more than half of this run's queue failed (likely blocked)
    if failures > len(stale_props) / 2:
        print("ERROR: >50% of scrapes failed — likely blocked by Redfin")
        sys.exit(1)


if __name__ == "__main__":
    main()
