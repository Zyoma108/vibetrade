"""
Shared utilities for analytics modules.

Extracts duplicated code from detector.py, price_surge.py, app.py, and runner.py.
"""

import numpy as np

OI_TREND_BARS = 3  # Number of OI data points for trend check


def timeframe_to_minutes(tf: str) -> int:
    """Convert timeframe string (e.g. '3m', '1h') to minutes."""
    if tf.endswith("m"):
        return int(tf[:-1])
    elif tf.endswith("h"):
        return int(tf[:-1]) * 60
    return 3  # sensible default for unknown formats


def calculate_oi_slope_pct(values: np.ndarray) -> float | None:
    """Calculate OI trend slope as a percentage of mean OI.

    Returns None if there aren't enough data points or mean OI <= 0.
    """
    if len(values) < 2:
        return None

    x = np.arange(len(values))
    slope = np.polyfit(x, values, 1)[0]
    mean_oi = np.mean(values)
    if mean_oi <= 0:
        return None

    return (slope * len(values)) / mean_oi * 100
