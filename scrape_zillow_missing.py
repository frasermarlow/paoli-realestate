#!/usr/bin/env python3
"""Scrape Zillow Zestimates for properties that don't have one yet."""
from __future__ import annotations

import logging
import random
import time

import config
from db import Estimate, SessionLocal, add_estimate, get_all_properties
from scraper import cleanup_driver, scrape_zillow

logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)

BROWSER_RESTART_EVERY = 6  # restart browser every N properties to avoid CAPTCHA


def main():
    session = SessionLocal()
    props = get_all_properties(session)

    # Find properties missing Zillow estimates
    missing = []
    for p in props:
        has_zillow = session.query(Estimate).filter_by(property_id=p.id, source="zillow").count()
        if has_zillow == 0 and p.zillow_url:
            missing.append(p)

    print(f"Properties missing Zillow estimates: {len(missing)}")
    if not missing:
        print("All done!")
        return

    successes = 0
    failures = 0

    try:
        for i, prop in enumerate(missing):
            # Restart browser periodically to avoid CAPTCHA buildup
            if i > 0 and i % BROWSER_RESTART_EVERY == 0:
                logger.info("Restarting browser to avoid CAPTCHA (after %d properties)", i)
                cleanup_driver()
                time.sleep(random.uniform(15, 25))

            logger.info("[%d/%d] Scraping Zillow for %s", i + 1, len(missing), prop.address)
            price = scrape_zillow(prop.zillow_url)

            if price:
                add_estimate(session, prop.id, "zillow", price)
                print(f"  Unit {prop.unit_number}: ${price:,.0f}")
                successes += 1
            else:
                print(f"  Unit {prop.unit_number}: FAILED")
                failures += 1

            # Generous delay between requests
            time.sleep(random.uniform(4, 8))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        cleanup_driver()
        session.close()

    print(f"\nDone: {successes} successes, {failures} failures")


if __name__ == "__main__":
    main()
