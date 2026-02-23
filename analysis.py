import logging
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import config
from db import SessionLocal, get_all_properties, get_estimates_for_property, get_sales_with_estimates

logger = logging.getLogger(__name__)


def _get_estimates_df() -> pd.DataFrame:
    """Load all estimates into a DataFrame."""
    session = SessionLocal()
    try:
        props = get_all_properties(session)
        rows = []
        for prop in props:
            for est in prop.estimates:
                rows.append({
                    "property_id": prop.id,
                    "address": prop.address,
                    "unit_number": prop.unit_number,
                    "source": est.source,
                    "estimated_price": est.estimated_price,
                    "captured_at": est.captured_at,
                })
        return pd.DataFrame(rows)
    finally:
        session.close()


def _get_errors_df() -> pd.DataFrame:
    """Load sales-vs-estimates error data into a DataFrame."""
    session = SessionLocal()
    try:
        data = get_sales_with_estimates(session)
        return pd.DataFrame(data)
    finally:
        session.close()


def calculate_errors() -> dict:
    """Calculate median and mean error by source."""
    df = _get_errors_df()
    if df.empty:
        logger.warning("No sales/estimate data available for error calculation")
        return {}

    results = {}
    for source in df["source"].unique():
        subset = df[df["source"] == source]
        results[source] = {
            "count": len(subset),
            "mean_error": subset["error"].mean(),
            "median_error": subset["error"].median(),
            "mean_pct_error": subset["pct_error"].mean(),
            "median_pct_error": subset["pct_error"].median(),
            "std_error": subset["error"].std(),
            "std_pct_error": subset["pct_error"].std(),
        }
    return results


def estimate_vs_actual_timeseries(save_path: str = None):
    """Generate a time-series plot of estimates vs actual sale prices."""
    df = _get_errors_df()
    if df.empty:
        logger.warning("No data for time-series plot")
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    for source, color in [("zillow", "blue"), ("redfin", "red")]:
        subset = df[df["source"] == source].sort_values("sale_date")
        if subset.empty:
            continue
        ax.plot(subset["sale_date"], subset["estimated_price"], "o-", color=color,
                label=f"{source.title()} Estimate", alpha=0.7)
        ax.plot(subset["sale_date"], subset["sale_price"], "s--", color=color,
                label=f"Actual (via {source.title()})", alpha=0.4)

    ax.set_xlabel("Sale Date")
    ax.set_ylabel("Price ($)")
    ax.set_title("Estimates vs Actual Sale Prices Over Time")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    save_path = save_path or os.path.join(config.DATA_DIR, "timeseries.png")
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    logger.info("Saved time-series plot to %s", save_path)


def accuracy_scatter(save_path: str = None):
    """Generate scatter plots of estimated vs actual price with trend lines."""
    df = _get_errors_df()
    if df.empty:
        logger.warning("No data for scatter plot")
        return

    sources = df["source"].unique()
    fig, axes = plt.subplots(1, len(sources), figsize=(7 * len(sources), 6), squeeze=False)

    for i, source in enumerate(sources):
        ax = axes[0, i]
        subset = df[df["source"] == source]

        ax.scatter(subset["sale_price"], subset["estimated_price"], alpha=0.7, s=60)

        # Perfect accuracy line
        min_val = min(subset["sale_price"].min(), subset["estimated_price"].min()) * 0.95
        max_val = max(subset["sale_price"].max(), subset["estimated_price"].max()) * 1.05
        ax.plot([min_val, max_val], [min_val, max_val], "k--", alpha=0.5, label="Perfect accuracy")

        # Trend line
        if len(subset) >= 2:
            z = np.polyfit(subset["sale_price"], subset["estimated_price"], 1)
            p = np.poly1d(z)
            x_line = np.linspace(min_val, max_val, 100)
            ax.plot(x_line, p(x_line), "r-", alpha=0.5, label=f"Trend (slope={z[0]:.2f})")

        ax.set_xlabel("Actual Sale Price ($)")
        ax.set_ylabel("Estimated Price ($)")
        ax.set_title(f"{source.title()} Accuracy")
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    save_path = save_path or os.path.join(config.DATA_DIR, "accuracy_scatter.png")
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    logger.info("Saved scatter plot to %s", save_path)


def error_distribution(save_path: str = None):
    """Generate box plots of percentage errors by source."""
    df = _get_errors_df()
    if df.empty:
        logger.warning("No data for error distribution plot")
        return

    fig, ax = plt.subplots(figsize=(8, 6))

    sources = sorted(df["source"].unique())
    data = [df[df["source"] == s]["pct_error"].values for s in sources]

    bp = ax.boxplot(data, labels=[s.title() for s in sources], patch_artist=True)
    colors = ["#4A90D9", "#D94A4A", "#4AD94A", "#D9D94A"]
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)

    ax.axhline(y=0, color="black", linestyle="--", alpha=0.5)
    ax.set_ylabel("Percentage Error (%)")
    ax.set_title("Distribution of Estimate Errors by Source")
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    save_path = save_path or os.path.join(config.DATA_DIR, "error_distribution.png")
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    logger.info("Saved error distribution plot to %s", save_path)


def generate_report():
    """Generate summary statistics and all plots."""
    print("\n=== Woodgate Estimate Accuracy Report ===\n")

    # Summary statistics
    errors = calculate_errors()
    if not errors:
        print("No sales data with matching estimates yet.")
        print("Add sales with 'python main.py add-sale' and ensure estimates exist.\n")
    else:
        for source, stats in errors.items():
            print(f"--- {source.title()} ---")
            print(f"  Comparisons:        {stats['count']}")
            print(f"  Mean Error:         ${stats['mean_error']:+,.0f}")
            print(f"  Median Error:       ${stats['median_error']:+,.0f}")
            print(f"  Mean % Error:       {stats['mean_pct_error']:+.1f}%")
            print(f"  Median % Error:     {stats['median_pct_error']:+.1f}%")
            print(f"  Std Dev Error:      ${stats['std_error']:,.0f}")
            print(f"  Std Dev % Error:    {stats['std_pct_error']:.1f}%")
            print()

        # Generate plots
        estimate_vs_actual_timeseries()
        accuracy_scatter()
        error_distribution()
        print(f"Plots saved to {config.DATA_DIR}/")

    # Estimates summary
    est_df = _get_estimates_df()
    if not est_df.empty:
        print(f"\nTotal estimates collected: {len(est_df)}")
        print(f"Properties with estimates: {est_df['property_id'].nunique()}")
        print(f"Date range: {est_df['captured_at'].min()} to {est_df['captured_at'].max()}")
        for source in est_df["source"].unique():
            count = len(est_df[est_df["source"] == source])
            print(f"  {source.title()}: {count} estimates")
    else:
        print("\nNo estimates collected yet. Run 'python main.py scrape' to start.")

    print()
