#!/usr/bin/env python3
"""Record Zillow Zestimates by hand, a batch at a time.

Zillow's bot protection blocks automated clients, so this script does not fetch
anything from Zillow. It opens each due unit in your normal browser and saves the
Zestimate you type in, to docs/data.json (the dashboard) and data/woodgate.db (the
only place Zestimate history is kept).

    /usr/bin/python3 update_zillow.py            # enter the next batch
    /usr/bin/python3 update_zillow.py --limit 5  # enter a smaller batch
    /usr/bin/python3 update_zillow.py --list     # show what is due, change nothing
    python3 update_zillow.py --notify            # macOS notification if anything is due
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import webbrowser
from datetime import date, timedelta

from zillow_alerts import compute_zillow_alerts

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROPERTIES_CSV = os.path.join(BASE_DIR, "properties.csv")
DATA_JSON = os.path.join(BASE_DIR, "docs", "data.json")

FRESHNESS_DAYS = 30        # a Zestimate older than this is due for a refresh
DEFAULT_BATCH = 15         # 15 a week covers all 60 units inside about a month
MIN_PLAUSIBLE = 150_000    # reject typos outside this range outright
MAX_PLAUSIBLE = 2_000_000
CONFIRM_CHANGE_PCT = 20    # ask for confirmation on a larger move than this


def load_data():
    with open(DATA_JSON) as f:
        return json.load(f)


def save_data(data):
    """Write data.json atomically so an interrupted save can't corrupt it."""
    tmp_path = DATA_JSON + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, DATA_JSON)


def load_zillow_urls():
    with open(PROPERTIES_CSV, newline="") as f:
        return {int(r["unit_number"]): r["zillow_url"] for r in csv.DictReader(f) if r["zillow_url"]}


def due_units(data, urls):
    """Units with a missing or stale Zestimate: post-sale alerts first, then oldest first."""
    cutoff = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    alerted = {a["unit"] for a in compute_zillow_alerts(data["properties"], data.get("sales", []))}
    due = [
        p for p in data["properties"]
        if p["unit"] in urls and (not p.get("zillow_date") or p["zillow_date"] < cutoff)
    ]
    return sorted(due, key=lambda p: (p["unit"] not in alerted, p.get("zillow_date") or "", p["unit"])), alerted


def parse_price(text):
    """Parse '512,300', '$512300' or '512k'. Returns None if it isn't a number."""
    cleaned = text.strip().lower().replace("$", "").replace(",", "")
    multiplier = 1
    if cleaned.endswith("k"):
        multiplier = 1000
        cleaned = cleaned[:-1]
    try:
        return round(float(cleaned) * multiplier)
    except ValueError:
        return None


def redfin_job_running():
    """The Redfin job rewrites data.json when it finishes, which would discard entries made meanwhile."""
    pattern = r"[Pp]ython[0-9.]* .*ci_update_redfin\.py$"
    return subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode == 0


def record(unit, price, session):
    """Append to the history database, then update the dashboard data."""
    from db import Property, add_estimate

    prop = session.query(Property).filter_by(unit_number=str(unit)).first()
    if prop is None:
        raise ValueError(f"Unit {unit} is not in the database")
    add_estimate(session, prop.id, "zillow", price)

    today = date.today().isoformat()
    data = load_data()  # re-read so we never write back an older copy
    target = next(p for p in data["properties"] if p["unit"] == unit)
    target["zillow"] = price
    target["zillow_date"] = today
    data["zillow_alerts"] = compute_zillow_alerts(data["properties"], data.get("sales", []))
    data["exported_at"] = f"{today}T00:00:00Z"
    save_data(data)


def prompt_for_price(prop):
    """Return a validated price, 'skip', or 'quit'."""
    prior = prop.get("zillow")
    while True:
        try:
            answer = input(f"  Zestimate (s = skip, q = quit) [last: {prior}]: ").strip()
        except EOFError:
            return "quit"
        if answer.lower() in ("q", "quit"):
            return "quit"
        if answer.lower() in ("s", "skip"):
            return "skip"
        price = parse_price(answer)
        if price is None or not MIN_PLAUSIBLE <= price <= MAX_PLAUSIBLE:
            print(f"  Enter a price between ${MIN_PLAUSIBLE:,} and ${MAX_PLAUSIBLE:,}.")
            continue
        if prior and abs(price - prior) / prior * 100 > CONFIRM_CHANGE_PCT:
            try:
                confirm = input(f"  ${price:,} is over {CONFIRM_CHANGE_PCT}% from ${prior:,.0f}. Keep it? (y/n): ")
            except EOFError:
                return "quit"
            if confirm.strip().lower() != "y":
                continue
        return price


def show_list(due, alerted, limit):
    print(f"{len(due)} of 60 units are due for a Zillow update. Next {min(limit, len(due))}:")
    for p in due[:limit]:
        note = "  (sold since last capture)" if p["unit"] in alerted else ""
        print(f"  Unit {p['unit']}: last captured {p.get('zillow_date') or 'never'}{note}")


def notify(count):
    print(f"{count} units are due for a Zillow update.")
    if count:
        message = f"{count} units need a Zillow update. Run update_zillow.py."
        script = f'display notification "{message}" with title "Woodgate tracker"'
        subprocess.run(["osascript", "-e", script], check=False)


def enter_batch(due, alerted, urls, limit):
    try:
        from db import SessionLocal
    except ImportError:
        sys.exit("Missing database packages. Run this with /usr/bin/python3.")

    session = SessionLocal()
    saved = 0
    batch = due[:limit]
    try:
        for i, prop in enumerate(batch, 1):
            unit = prop["unit"]
            if redfin_job_running():
                print("The Redfin job is running and would overwrite new entries. Try again when it finishes.")
                break
            note = " (sold since last capture)" if unit in alerted else ""
            print(f"\n[{i}/{len(batch)}] Unit {unit}{note}: {urls[unit]}")
            webbrowser.open(urls[unit])
            result = prompt_for_price(prop)
            if result == "quit":
                break
            if result == "skip":
                continue
            record(unit, result, session)
            saved += 1
            print(f"  Saved ${result:,}")
    finally:
        session.close()

    print(f"\nSaved {saved} Zestimates. The next weekly job will publish them to the dashboard.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=DEFAULT_BATCH, help="units to enter this session")
    parser.add_argument("--list", action="store_true", help="show what is due and exit")
    parser.add_argument("--notify", action="store_true", help="send a macOS notification if units are due")
    args = parser.parse_args()

    urls = load_zillow_urls()
    due, alerted = due_units(load_data(), urls)

    if args.notify:
        notify(len(due))
    elif args.list:
        show_list(due, alerted, args.limit)
    elif due:
        enter_batch(due, alerted, urls, args.limit)
    else:
        print("All Zestimates are up to date.")


if __name__ == "__main__":
    main()
