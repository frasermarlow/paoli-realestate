"""Scrape historical sales from Redfin and Zillow for all 60 Woodgate units.

Compares results against existing hoa_sales.csv records.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RESULTS_PATH = os.path.join(config.BASE_DIR, "data", "scraped_sales_history.json")
REPORT_PATH = os.path.join(config.BASE_DIR, "data", "sales_comparison_report.txt")
HOA_CSV = os.path.join(config.BASE_DIR, "hoa_sales.csv")


def load_properties():
    """Load unit -> URLs from properties.csv."""
    props = []
    with open(config.PROPERTIES_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props.append({
                "unit": int(row["unit_number"]),
                "address": row["address"],
                "redfin_url": row["redfin_url"],
                "zillow_url": row["zillow_url"],
            })
    return props


def load_hoa_sales():
    """Load existing hoa_sales.csv. Returns dict: unit -> list of (date, price)."""
    sales = {}
    with open(HOA_CSV, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            rec_type = row[0].strip('"')
            if rec_type != "SALE":
                continue
            unit = int(row[1].strip('"'))
            date_str = row[2].strip('"')
            price = int(row[3].strip('"'))
            sales.setdefault(unit, []).append({"date": date_str, "price": price})
    for unit in sales:
        sales[unit].sort(key=lambda s: s["date"])
    return sales


def get_headers():
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def parse_price(text):
    """Extract numeric price from text like '$425,000'."""
    if not text or text.strip() in ("", "—", "-"):
        return None
    cleaned = text.strip().replace(",", "").replace("$", "")
    match = re.search(r"(\d+)", cleaned)
    return int(match.group(1)) if match else None


def parse_date_redfin(text):
    """Parse Redfin date like 'Dec 23, 2020' to 'YYYY-MM-DD'."""
    try:
        dt = datetime.strptime(text.strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def scrape_redfin_sales(url):
    """Scrape sale history from a Redfin property page."""
    try:
        resp = requests.get(url, headers=get_headers(), timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to fetch %s: %s", url, e)
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    panel = soup.select_one(".sale-history-panel")
    if not panel:
        logger.warning("No sale history panel found: %s", url)
        return []

    sales = []
    rows = panel.select(".BasicTable__row")
    for row in rows:
        date_el = row.select_one(".date")
        event_el = row.select_one(".event")
        price_el = row.select_one(".price")
        if not (date_el and event_el):
            continue

        event = event_el.get_text(strip=True)
        if event != "Sold":
            continue

        date_str = parse_date_redfin(date_el.get_text(strip=True))
        if not date_str:
            continue

        # Remove sq ft subtext before parsing price
        if price_el:
            for sub in price_el.select(".subtext"):
                sub.decompose()
            price = parse_price(price_el.get_text(strip=True))
        else:
            price = None

        if price:
            sales.append({"date": date_str, "price": price})

    return sales


def scrape_all_redfin(properties):
    """Scrape Redfin sales history for all properties."""
    results = {}
    for i, prop in enumerate(properties):
        unit = prop["unit"]
        url = prop["redfin_url"]
        logger.info("Redfin %d/%d: Unit %d", i + 1, len(properties), unit)

        sales = scrape_redfin_sales(url)
        if sales is not None:
            results[unit] = sales
            logger.info("  Found %d sales", len(sales))
        else:
            results[unit] = []
            logger.warning("  Failed to scrape")

        if i < len(properties) - 1:
            time.sleep(random.uniform(1.0, 2.0))

    return results


def scrape_zillow_sales(url, driver):
    """Scrape sale history from a Zillow property page using Selenium."""
    try:
        driver.get(url)
        time.sleep(random.uniform(4, 7))

        page_source = driver.page_source
        if "Access to this page has been denied" in page_source:
            logger.warning("Zillow CAPTCHA detected for %s", url)
            return None

        soup = BeautifulSoup(page_source, "lxml")

        # Zillow price history is typically in a section with "Price history"
        # Look for table rows with Sold events
        sales = []

        # Try to find the price history table
        # Zillow uses various structures, let's try multiple approaches

        # Approach 1: Look for data-testid elements
        history_rows = soup.select('[data-testid="price-history"] tr, [data-testid="priceHistory"] tr')

        # Approach 2: Look for "Price history" heading and nearby table
        if not history_rows:
            for heading in soup.find_all(["h2", "h3", "h4", "h5", "span"]):
                if "Price history" in heading.get_text():
                    parent = heading.find_parent("section") or heading.find_parent("div")
                    if parent:
                        history_rows = parent.select("tr")
                        break

        # Approach 3: Regex fallback on full page text
        if not history_rows:
            text = soup.get_text()
            # Pattern: date Sold price
            pattern = re.compile(
                r"(\d{1,2}/\d{1,2}/\d{4})\s+Sold\s+\$([0-9,]+)",
                re.IGNORECASE
            )
            for match in pattern.finditer(text):
                try:
                    dt = datetime.strptime(match.group(1), "%m/%d/%Y")
                    price = int(match.group(2).replace(",", ""))
                    sales.append({"date": dt.strftime("%Y-%m-%d"), "price": price})
                except ValueError:
                    pass
            return sales

        for row in history_rows:
            cells = row.select("td, th")
            if len(cells) < 3:
                continue
            row_text = [c.get_text(strip=True) for c in cells]

            # Check if this is a "Sold" event
            event_text = " ".join(row_text).lower()
            if "sold" not in event_text:
                continue

            # Find date and price
            date_str = None
            price = None
            for cell_text in row_text:
                # Date patterns
                date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", cell_text)
                if date_match:
                    try:
                        dt = datetime.strptime(date_match.group(1), "%m/%d/%Y")
                        date_str = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
                # Price
                if "$" in cell_text and not price:
                    price = parse_price(cell_text)

            if date_str and price:
                sales.append({"date": date_str, "price": price})

        return sales

    except Exception as e:
        logger.error("Zillow scrape error for %s: %s", url, e)
        return None


def scrape_all_zillow(properties):
    """Scrape Zillow sales history for all properties using Selenium."""
    import undetected_chromedriver as uc

    results = {}
    driver = None
    request_count = 0

    try:
        opts = uc.ChromeOptions()
        driver = uc.Chrome(options=opts, headless=False)

        for i, prop in enumerate(properties):
            unit = prop["unit"]
            url = prop["zillow_url"]
            logger.info("Zillow %d/%d: Unit %d", i + 1, len(properties), unit)

            # Restart browser every 6 requests to avoid CAPTCHA
            if request_count > 0 and request_count % 6 == 0:
                logger.info("Restarting browser to avoid CAPTCHA...")
                driver.quit()
                time.sleep(random.uniform(10, 15))
                driver = uc.Chrome(options=opts, headless=False)

            sales = scrape_zillow_sales(url, driver)
            request_count += 1

            if sales is not None:
                results[unit] = sales
                logger.info("  Found %d sales", len(sales))
            else:
                results[unit] = []
                logger.warning("  Failed to scrape (CAPTCHA or error)")

            if i < len(properties) - 1:
                time.sleep(random.uniform(3, 6))

    finally:
        if driver:
            driver.quit()

    return results


def compare_sales(hoa_sales, redfin_sales, zillow_sales):
    """Compare scraped sales against HOA records. Returns report lines."""
    lines = []
    all_units = sorted(set(
        list(hoa_sales.keys()) +
        list(redfin_sales.keys()) +
        list(zillow_sales.keys())
    ))

    new_sales = []  # Sales found online but not in HOA
    date_mismatches = []  # Same sale, different dates
    missing_online = []  # In HOA but not found online
    stats = {"matched": 0, "date_mismatch": 0, "new": 0, "missing": 0}

    for unit in all_units:
        hoa = hoa_sales.get(unit, [])
        # Deduplicate Redfin/Zillow records (same date+price)
        redfin_raw = redfin_sales.get(unit, [])
        redfin = []
        seen_r = set()
        for r in redfin_raw:
            key = (r["date"], r["price"])
            if key not in seen_r:
                seen_r.add(key)
                redfin.append(r)
        zillow_raw = zillow_sales.get(unit, [])
        zillow = []
        seen_z = set()
        for z in zillow_raw:
            key = (z["date"], z["price"])
            if key not in seen_z:
                seen_z.add(key)
                zillow.append(z)

        # For comparison, match sales by price (within $500) and approximate date (within 60 days)
        hoa_matched = set()
        redfin_matched = set()
        zillow_matched = set()

        def dates_close(d1, d2, days=60):
            """Check if two YYYY-MM-DD date strings are within N days."""
            try:
                dt1 = datetime.strptime(d1, "%Y-%m-%d")
                dt2 = datetime.strptime(d2, "%Y-%m-%d")
                return abs((dt1 - dt2).days) <= days
            except ValueError:
                return False

        # Match HOA sales to Redfin sales (price within 1% or $1000)
        for hi, h in enumerate(hoa):
            for ri, r in enumerate(redfin):
                if ri in redfin_matched:
                    continue
                price_diff = abs(h["price"] - r["price"])
                pct_diff = price_diff / max(h["price"], 1) * 100
                if (price_diff <= 1000 or pct_diff <= 1.0) and dates_close(h["date"], r["date"]):
                    hoa_matched.add(hi)
                    redfin_matched.add(ri)
                    mismatch_info = {
                        "unit": unit, "hoa_price": h["price"], "redfin_price": r["price"],
                        "hoa_date": h["date"], "redfin_date": r["date"],
                        "source": "redfin"
                    }
                    if h["date"] != r["date"] or h["price"] != r["price"]:
                        date_mismatches.append(mismatch_info)
                        stats["date_mismatch"] += 1
                    else:
                        stats["matched"] += 1
                    break

        # Match HOA sales to Zillow sales
        for hi, h in enumerate(hoa):
            if hi in hoa_matched:
                continue
            for zi, z in enumerate(zillow):
                if zi in zillow_matched:
                    continue
                price_diff = abs(h["price"] - z["price"])
                pct_diff = price_diff / max(h["price"], 1) * 100
                if (price_diff <= 1000 or pct_diff <= 1.0) and dates_close(h["date"], z["date"]):
                    hoa_matched.add(hi)
                    zillow_matched.add(zi)
                    mismatch_info = {
                        "unit": unit, "hoa_price": h["price"], "zillow_price": z["price"],
                        "hoa_date": h["date"], "zillow_date": z["date"],
                        "source": "zillow"
                    }
                    if h["date"] != z["date"] or h["price"] != z["price"]:
                        date_mismatches.append(mismatch_info)
                        stats["date_mismatch"] += 1
                    else:
                        stats["matched"] += 1
                    break

        # Redfin sales not in HOA
        for ri, r in enumerate(redfin):
            if ri not in redfin_matched:
                # Check if it's in zillow too (corroborated)
                corroborated = any(
                    abs(r["price"] - z["price"]) <= 1000 and dates_close(r["date"], z["date"])
                    for z in zillow
                )
                new_sales.append({
                    "unit": unit, "date": r["date"], "price": r["price"],
                    "source": "redfin", "corroborated": corroborated
                })
                stats["new"] += 1

        # Zillow sales not in HOA and not already counted from Redfin
        for zi, z in enumerate(zillow):
            if zi not in zillow_matched:
                # Check if already found via Redfin
                already_found = any(
                    abs(z["price"] - ns["price"]) <= 1000 and dates_close(z["date"], ns["date"])
                    for ns in new_sales if ns["unit"] == unit
                )
                if not already_found:
                    new_sales.append({
                        "unit": unit, "date": z["date"], "price": z["price"],
                        "source": "zillow", "corroborated": False
                    })
                    stats["new"] += 1

        # HOA sales not found in either source
        for hi, h in enumerate(hoa):
            if hi not in hoa_matched:
                missing_online.append({"unit": unit, "date": h["date"], "price": h["price"]})
                stats["missing"] += 1

    # Build report
    lines.append("=" * 70)
    lines.append("WOODGATE SALES COMPARISON REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"HOA records: {sum(len(v) for v in hoa_sales.values())} sales")
    lines.append(f"Redfin records: {sum(len(v) for v in redfin_sales.values())} sales")
    lines.append(f"Zillow records: {sum(len(v) for v in zillow_sales.values())} sales")
    lines.append("")
    lines.append(f"Exact matches: {stats['matched']}")
    lines.append(f"Date mismatches (same sale, different date): {stats['date_mismatch']}")
    lines.append(f"New sales (online but not in HOA): {stats['new']}")
    lines.append(f"Missing online (in HOA but not found): {stats['missing']}")
    lines.append("")

    if date_mismatches:
        lines.append("-" * 70)
        lines.append("MISMATCHES (date and/or price differ)")
        lines.append("-" * 70)
        for dm in sorted(date_mismatches, key=lambda x: (x["unit"], x["hoa_date"])):
            src = dm["source"]
            other_date = dm.get(f"{src}_date")
            other_price = dm.get(f"{src}_price", dm.get("hoa_price"))
            hoa_price = dm["hoa_price"]
            date_note = ""
            if dm["hoa_date"] != other_date:
                date_note = f"date: HOA {dm['hoa_date']} vs {src.title()} {other_date}"
            price_note = ""
            if hoa_price != other_price:
                price_note = f"price: HOA ${hoa_price:,} vs {src.title()} ${other_price:,}"
            detail = " | ".join(filter(None, [date_note, price_note]))
            lines.append(f"  Unit {dm['unit']:3d} | ${hoa_price:>9,} | {detail}")
        lines.append("")

    if new_sales:
        lines.append("-" * 70)
        lines.append("NEW SALES (found online, not in HOA records)")
        lines.append("-" * 70)
        for ns in sorted(new_sales, key=lambda x: (x["unit"], x["date"])):
            corr = " [corroborated]" if ns["corroborated"] else ""
            lines.append(
                f"  Unit {ns['unit']:3d} | {ns['date']} | ${ns['price']:>9,} | "
                f"{ns['source'].title()}{corr}"
            )
        lines.append("")

    if missing_online:
        lines.append("-" * 70)
        lines.append("MISSING ONLINE (in HOA but not found on Redfin/Zillow)")
        lines.append("-" * 70)
        for mo in sorted(missing_online, key=lambda x: (x["unit"], x["date"])):
            lines.append(
                f"  Unit {mo['unit']:3d} | {mo['date']} | ${mo['price']:>9,}"
            )
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape sales history from Redfin and Zillow")
    parser.add_argument("--redfin-only", action="store_true", help="Only scrape Redfin")
    parser.add_argument("--zillow-only", action="store_true", help="Only scrape Zillow")
    parser.add_argument("--compare-only", action="store_true",
                        help="Skip scraping, compare from saved results")
    args = parser.parse_args()

    properties = load_properties()
    hoa_sales = load_hoa_sales()

    if args.compare_only:
        if os.path.exists(RESULTS_PATH):
            with open(RESULTS_PATH) as f:
                saved = json.load(f)
            redfin_sales = {int(k): v for k, v in saved.get("redfin", {}).items()}
            zillow_sales = {int(k): v for k, v in saved.get("zillow", {}).items()}
        else:
            print(f"No saved results at {RESULTS_PATH}")
            return
    else:
        redfin_sales = {}
        zillow_sales = {}

        if not args.zillow_only:
            logger.info("Starting Redfin scrape for %d properties...", len(properties))
            redfin_sales = scrape_all_redfin(properties)

        if not args.redfin_only:
            logger.info("Starting Zillow scrape for %d properties...", len(properties))
            zillow_sales = scrape_all_zillow(properties)

        # Save raw results
        os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
        saved = {}
        if os.path.exists(RESULTS_PATH):
            with open(RESULTS_PATH) as f:
                saved = json.load(f)
        if redfin_sales:
            saved["redfin"] = {str(k): v for k, v in redfin_sales.items()}
        if zillow_sales:
            saved["zillow"] = {str(k): v for k, v in zillow_sales.items()}
        with open(RESULTS_PATH, "w") as f:
            json.dump(saved, f, indent=2)
        logger.info("Saved raw results to %s", RESULTS_PATH)

    # Compare
    report = compare_sales(hoa_sales, redfin_sales, zillow_sales)
    print(report)

    with open(REPORT_PATH, "w") as f:
        f.write(report)
    logger.info("Report saved to %s", REPORT_PATH)


if __name__ == "__main__":
    main()
