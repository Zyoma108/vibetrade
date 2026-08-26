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


def oi_trend_passes(
    oi_values, oi_declining_enabled: bool, oi_slope_min_pct: float,
) -> tuple[bool, str | None, str | None]:
    """Решение OI-гейта по готовому ряду значений: (прошёл, stage, reason).

    Единственная реализация на боевой детектор и бэктест. До 26.08.2026 движок
    бэктеста повторял эту логику своим кодом, и она дважды разъезжалась с боевой:
    сначала из неё выпала проверка `oi_declining` (завысила прошлые свипы по
    RR/partial-close/retracement), потом — учёт `oi_filter_enabled`. Гейт целиком
    включается флагом `oi_filter_enabled` на стороне вызывающего: здесь только
    содержательная часть, без обращения к БД, чтобы её мог звать и синхронный
    движок, и асинхронный детектор.
    """
    if oi_values is None:
        return False, None, None

    # Последняя точка ниже предпоследней — приток уже иссякает
    if oi_declining_enabled and len(oi_values) >= 2 and oi_values[-1] < oi_values[-2]:
        return False, "oi_declining", "OI снижается — последняя точка ниже предпоследней"

    slope_pct = calculate_oi_slope_pct(np.asarray(oi_values))
    if slope_pct is None:
        return False, None, None

    if slope_pct < oi_slope_min_pct:
        return (
            False,
            "oi_slope_low",
            f"наклон OI {slope_pct:.1f}% < минимума {oi_slope_min_pct}%",
        )

    return True, None, None
