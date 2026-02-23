#!/usr/bin/env python3
"""CLI entry point for the Woodgate property monitoring system."""

import argparse
import sys
from datetime import datetime

import config  # noqa: F401 — triggers logging setup
from db import (
    Property,
    add_sale,
    get_all_properties,
    get_session,
    init_db,
    seed_db,
)


def cmd_init(args):
    """Initialize the database and seed properties from CSV."""
    init_db()
    count = seed_db()
    print(f"Database initialized. {count} properties seeded from {config.PROPERTIES_CSV}")

    session = get_session()
    try:
        props = get_all_properties(session)
        with_urls = sum(1 for p in props if p.zillow_url or p.redfin_url)
        print(f"Properties with URLs configured: {with_urls}/{len(props)}")
        if with_urls == 0:
            print("\nNote: No URLs are configured yet.")
            print("Edit properties.csv with Zillow/Redfin URLs and re-run 'init' to update.")
    finally:
        session.close()


def cmd_scrape(args):
    """Run the scraper on a batch of properties."""
    from scraper import scrape_batch

    session = get_session()
    try:
        if args.all:
            properties = [p for p in get_all_properties(session) if p.zillow_url or p.redfin_url]
        else:
            from scheduler import get_next_batch
            properties = get_next_batch(args.batch)

        if not properties:
            print("No properties with URLs configured. Edit properties.csv and run 'init'.")
            return

        print(f"Scraping {len(properties)} properties...")
        results = scrape_batch(properties)

        successes = sum(1 for r in results if r.success)
        failures = sum(1 for r in results if not r.success)
        print(f"\nDone: {successes} estimates collected, {failures} failures")

        for r in results:
            status = f"${r.price:,.0f}" if r.success else f"FAILED: {r.error_msg}"
            print(f"  Property {r.property_id} ({r.source}): {status}")
    finally:
        session.close()


def cmd_add_sale(args):
    """Interactively add a sale record."""
    session = get_session()
    try:
        properties = get_all_properties(session)
        if not properties:
            print("No properties in database. Run 'init' first.")
            return

        print("Properties:")
        for p in properties:
            print(f"  [{p.id}] Unit {p.unit_number}: {p.address}")

        prop_id = int(input("\nEnter property ID: "))
        prop = session.query(Property).get(prop_id)
        if not prop:
            print(f"Property {prop_id} not found.")
            return

        print(f"\nRecording sale for: {prop.address}")
        sale_price = float(input("Sale price (e.g. 425000): $").replace(",", ""))

        asking_input = input("Asking price (press Enter to skip): $").strip()
        asking_price = float(asking_input.replace(",", "")) if asking_input else None

        date_str = input("Sale date (YYYY-MM-DD): ").strip()
        sale_date = datetime.strptime(date_str, "%Y-%m-%d")

        add_sale(session, prop_id, sale_price, sale_date, asking_price)
        print(f"\nSale recorded: {prop.address} sold for ${sale_price:,.0f} on {date_str}")
    finally:
        session.close()


def cmd_analyze(args):
    """Generate analysis and plots."""
    from analysis import generate_report
    generate_report()


def cmd_schedule(args):
    """Start the automated scheduler."""
    from scheduler import start_scheduler
    start_scheduler()


def cmd_status(args):
    """Show current database status."""
    session = get_session()
    try:
        from db import Estimate, Sale
        props = get_all_properties(session)
        estimates = session.query(Estimate).count()
        sales = session.query(Sale).count()
        with_urls = sum(1 for p in props if p.zillow_url or p.redfin_url)

        print(f"Properties:  {len(props)} ({with_urls} with URLs)")
        print(f"Estimates:   {estimates}")
        print(f"Sales:       {sales}")

        if estimates > 0:
            latest = session.query(Estimate).order_by(Estimate.captured_at.desc()).first()
            print(f"Last scrape: {latest.captured_at}")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(
        description="Woodgate Development Sales Price Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python main.py init              Initialize DB and seed properties
  python main.py scrape --batch 3  Scrape 3 properties
  python main.py scrape --all      Scrape all properties with URLs
  python main.py add-sale          Manually record a sale
  python main.py analyze           Generate analysis report
  python main.py schedule          Start automated daily scraping
  python main.py status            Show database status
""",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Initialize DB and seed properties from CSV")

    scrape_parser = subparsers.add_parser("scrape", help="Scrape property estimates")
    scrape_group = scrape_parser.add_mutually_exclusive_group()
    scrape_group.add_argument("--all", action="store_true", help="Scrape all properties with URLs")
    scrape_group.add_argument("--batch", type=int, default=config.BATCH_SIZE,
                              help=f"Number of properties to scrape (default: {config.BATCH_SIZE})")

    subparsers.add_parser("add-sale", help="Manually record a property sale")
    subparsers.add_parser("analyze", help="Generate analysis report and plots")
    subparsers.add_parser("schedule", help="Start automated daily scraper")
    subparsers.add_parser("status", help="Show database status")

    args = parser.parse_args()
    commands = {
        "init": cmd_init,
        "scrape": cmd_scrape,
        "add-sale": cmd_add_sale,
        "analyze": cmd_analyze,
        "schedule": cmd_schedule,
        "status": cmd_status,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
