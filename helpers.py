"""
Helper functions and constants for the crypto dashboard.
"""

import pandas as pd


def moving_average(series: pd.Series, window: int) -> pd.Series:
    """Calculate moving average for a given window size."""
    return series.rolling(window=window, min_periods=1).mean()


MARKET_CAP_BINS = {
    "All": (0, float("inf")),
    "Large-cap  (> $10B)":  (10_000_000_000, float("inf")),
    "Mid-cap  ($1B – $10B)": (1_000_000_000, 10_000_000_000),
    "Small-cap  (< $1B)":   (0, 1_000_000_000),
}
