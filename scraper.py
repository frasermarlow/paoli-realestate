from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

import config
from db import SessionLocal, add_estimate

logger = logging.getLogger(__name__)

# Shared undetected Chrome driver (created lazily)
_driver = None


def _get_driver():
    """Get or create a shared undetected Chrome driver."""
    global _driver
    if _driver is None:
        opts = uc.ChromeOptions()
        _driver = uc.Chrome(options=opts, headless=False)
    return _driver


def cleanup_driver():
    """Shut down the shared Chrome driver."""
    global _driver
    if _driver is not None:
        _driver.quit()
        _driver = None


@dataclass
class ScrapeResult:
    property_id: int
    source: str
    success: bool
    price: float | None = None
    error_msg: str | None = None


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(config.USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def _fetch_page(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=_get_headers(), timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        logger.error("Failed to fetch %s: %s", url, e)
        return None


def _parse_price(text: str) -> float | None:
    """Extract a numeric price from text like '$425,000' or '$425K'."""
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


def scrape_zillow(url: str) -> float | None:
    """Extract Zestimate from a Zillow property page using Selenium."""
    driver = _get_driver()
    try:
        driver.get(url)
        time.sleep(random.uniform(5, 8))

        # Check for CAPTCHA/block page and retry with fresh browser
        if "Access to this page has been denied" in driver.page_source:
            logger.warning("Zillow CAPTCHA detected, restarting browser...")
            cleanup_driver()
            time.sleep(random.uniform(10, 15))
            driver = _get_driver()
            driver.get(url)
            time.sleep(random.uniform(6, 10))

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # Primary selector: data-testid="primary-zestimate"
        el = soup.select_one('[data-testid="primary-zestimate"]')
        if el:
            price = _parse_price(el.get_text())
            if price:
                return price

        # Fallback selectors
        for selector in [
            '[data-testid="price"]',
            'span[data-testid="zestimate-text"]',
        ]:
            el = soup.select_one(selector)
            if el:
                price = _parse_price(el.get_text())
                if price:
                    return price

        # Last resort: regex on page text
        text = soup.get_text()
        zestimate_pattern = re.compile(r"Zestimate[^$]*\$([0-9,]+)", re.IGNORECASE)
        match = zestimate_pattern.search(text)
        if match:
            return _parse_price(match.group(1))

        logger.warning("Could not find Zestimate on page: %s", url)
        return None
    except Exception as e:
        logger.error("Selenium error fetching Zillow %s: %s", url, e)
        return None


def scrape_redfin(url: str) -> float | None:
    """Extract Redfin estimate from a Redfin property page."""
    soup = _fetch_page(url)
    if not soup:
        return None

    # Redfin estimate selectors
    for selector in [
        'div[class*="redfin-estimate"] span[class*="value"]',
        'div[data-rf-test-id="avmLdpPrice"]',
        'span[class*="EstimatePrice"]',
    ]:
        el = soup.select_one(selector)
        if el:
            price = _parse_price(el.get_text())
            if price:
                return price

    # Fallback: search for "Redfin Estimate" text
    estimate_pattern = re.compile(r"Redfin Estimate[^$]*\$([0-9,]+)", re.IGNORECASE)
    match = estimate_pattern.search(soup.get_text())
    if match:
        return _parse_price(match.group(1))

    logger.warning("Could not find Redfin estimate on page: %s", url)
    return None


def scrape_property(prop) -> list[ScrapeResult]:
    """Scrape both Zillow and Redfin for a single property. Returns list of ScrapeResults."""
    results = []
    session = SessionLocal()

    try:
        if prop.zillow_url:
            price = scrape_zillow(prop.zillow_url)
            if price:
                add_estimate(session, prop.id, "zillow", price)
                results.append(ScrapeResult(prop.id, "zillow", True, price))
                logger.info("Zillow estimate for %s: $%.0f", prop.address, price)
            else:
                results.append(ScrapeResult(prop.id, "zillow", False, error_msg="Could not extract Zestimate"))
            # Longer delay for Zillow to avoid CAPTCHA
            time.sleep(random.uniform(3, 6))

        if prop.redfin_url:
            price = scrape_redfin(prop.redfin_url)
            if price:
                add_estimate(session, prop.id, "redfin", price)
                results.append(ScrapeResult(prop.id, "redfin", True, price))
                logger.info("Redfin estimate for %s: $%.0f", prop.address, price)
            else:
                results.append(ScrapeResult(prop.id, "redfin", False, error_msg="Could not extract Redfin estimate"))
            time.sleep(random.uniform(config.MIN_DELAY, config.MAX_DELAY))

        if not prop.zillow_url and not prop.redfin_url:
            results.append(ScrapeResult(prop.id, "none", False, error_msg="No URLs configured"))
    finally:
        session.close()

    return results


def scrape_batch(properties) -> list[ScrapeResult]:
    """Scrape a batch of properties with rate limiting between each."""
    all_results = []
    try:
        for i, prop in enumerate(properties):
            logger.info("Scraping property %d/%d: %s", i + 1, len(properties), prop.address)
            results = scrape_property(prop)
            all_results.extend(results)

            if i < len(properties) - 1:
                delay = random.uniform(config.MIN_DELAY, config.MAX_DELAY)
                logger.debug("Sleeping %.1fs before next property", delay)
                time.sleep(delay)
    finally:
        cleanup_driver()

    successes = sum(1 for r in all_results if r.success)
    failures = sum(1 for r in all_results if not r.success)
    logger.info("Batch complete: %d successes, %d failures", successes, failures)
    return all_results
