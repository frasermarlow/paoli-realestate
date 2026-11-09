"""Shared logic for the dashboard's "Zillow data collection needed" banner.

Standard library only, so the Redfin job can import it without the database stack.
"""
from __future__ import annotations


def compute_zillow_alerts(properties, sales):
    """Return an alert for each unit whose latest sale is newer than its latest Zestimate."""
    latest_sale = {}
    for sale in sales:
        current = latest_sale.get(sale["unit"])
        if current is None or sale["date"] > current["date"]:
            latest_sale[sale["unit"]] = sale

    alerts = []
    for prop in properties:
        sale = latest_sale.get(prop["unit"])
        if sale is None:
            continue
        zillow_date = prop.get("zillow_date")
        if not zillow_date or sale["date"] > zillow_date:
            alerts.append({
                "unit": prop["unit"],
                "sale_date": sale["date"],
                "sale_price": sale["price"],
                "zillow_date": zillow_date,
            })
    return alerts
