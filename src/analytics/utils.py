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


# Границы адаптивного стопа, % от цены входа. Не конфиг, потому что это не
# настройка стратегии, а область определения: ниже 2% стоп сидит в
# проскальзывании и комиссии (замер 26 суток: выход по стопу отклоняется от
# заявленного медианно на 0.01 п.п., но круговая комиссия — 0.11% в цене), выше
# 10% сам свип стопов показал ухудшение на обеих БД (3-8%, оптимум 4-5%).
ADAPTIVE_STOP_MIN_PCT = 2.0
ADAPTIVE_STOP_MAX_PCT = 10.0


def adaptive_stop_pct(
    range_pct: float | None,
    mult: float,
    fixed_pct: float,
    min_pct: float = ADAPTIVE_STOP_MIN_PCT,
    max_pct: float = ADAPTIVE_STOP_MAX_PCT,
) -> float:
    """Стоп в % от входа: `mult` размахов sustain-окна, либо фиксированный.

    `mult <= 0` или неизвестный размах — возвращается `fixed_pct`, то есть
    поведение до 23.09.2026. Одна реализация на бэктест и прод: разъехавшаяся
    копия гейта уже дважды стоила неверных выводов (AGENTS.md, правило 2).

    Зачем вообще. Стоп зафиксирован в процентах от цены, а размах sustain-окна у
    монет гуляет от 0.95% до 5.0% — то есть один и тот же стоп стоит от 1.4 до
    3.8 размаха. Замер 23.09.2026 на двух БД: самый шумный квартиль (стоп ~1.4
    размаха, внутри шума) — худший на обеих (E[R] -0.001 против +0.253 на
    27.08-22.09; +0.434 против +0.973 на 10-25.08), корреляция R с отношением
    стоп/шум положительна на обеих (+0.090 и +0.170). Значимости нет ни там, ни
    там — но направление воспроизвелось, а сама величина детектором до сих пор
    не использовалась.

    Это НЕ ATR-адаптивный стоп, отвергнутый в `docs/decisions.md`: там мерой был
    ИСТОРИЧЕСКИЙ ATR, непоказательный в момент пампа. Здесь мера — размах того же
    sustain-окна, который детектор уже считает для `max_window_range_pct`, то
    есть современная сигналу, а не историческая.
    """
    if mult <= 0 or range_pct is None or range_pct <= 0:
        return fixed_pct
    return min(max(range_pct * mult, min_pct), max_pct)
