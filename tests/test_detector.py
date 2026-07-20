"""
Tests for SetupDetector core logic — volume pattern, price trend, signal building.

These are pure-function tests; no database needed.
"""

import numpy as np
import pytest

from src.analytics.detector import SetupDetector
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct
from src.config import StrategyConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _detector(**overrides) -> SetupDetector:
    """Create a detector with default test config, optionally overriding fields."""
    params = {
        "baseline_bars": 70,
        "volume_surge_mult": 15.0,
        "oi_slope_min_pct": 1.0,
        "price_growth_min_pct": 1.0,
        "price_growth_max_pct": 12.0,
        "exhaustion_gain_pct": 5.0,
        "exhaustion_pos_ratio": 0.7,
        "max_hourly_drop_pct": 10.0,
        "dump_volume_mult": 3.0,
        "smooth_max_ratio": 5.0,
        "min_baseline_volume_usdt": 0.0,
    }
    params.update(overrides)
    cfg = StrategyConfig(**params)
    return SetupDetector(cfg, timeframe="3m")


def _candles(
    count: int,
    volume: float | list[float] = 100_000.0,
    price: float = 1.0,
    open_price: float | None = None,
    high_price: float | None = None,
    low_price: float | None = None,
    close_price: float | None = None,
    price_path: list[float] | None = None,
    volume_path: list[float] | None = None,
) -> list[dict]:
    """Build a list of candle dicts.

    If ``price_path`` is provided, each candle's open/close follows that path
    (open[i] = price_path[i], close[i] = price_path[i+1] if i < len-1 else last).
    """
    candles = []
    for i in range(count):
        if price_path:
            o = price_path[i]
            c = price_path[i + 1] if i + 1 < len(price_path) else price_path[-1]
            h = max(o, c) * 1.001
            l = min(o, c) * 0.999
        else:
            o = open_price if open_price is not None else price
            c = close_price if close_price is not None else price
            h = high_price if high_price is not None else price * 1.001
            l = low_price if low_price is not None else price * 0.999

        vol = volume[i] if isinstance(volume, list) else volume
        candles.append({"open": o, "high": h, "low": l, "close": c, "volume": vol})
    return candles


# ---------------------------------------------------------------------------
# Volume pattern
# ---------------------------------------------------------------------------


class TestVolumePattern:
    """check_volume_pattern — the core volume surge detection."""

    def test_all_sustain_bars_above_threshold(self):
        """All 4 sustain bars exceed baseline_median × volume_surge_mult."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        # baseline: 5 bars at vol=100 → median=100, threshold=300
        # sustain: 4 bars at vol=500 each
        candles = _candles(9, volume=[100] * 5 + [500] * 4)
        assert d.check_volume_pattern(candles)

    def test_one_sustain_bar_below_threshold_fails(self):
        """Even one sustain bar below threshold → fail."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        candles = _candles(9, volume=[100] * 5 + [500, 500, 299, 500])
        assert not d.check_volume_pattern(candles)

    def test_baseline_zero_volume_fails(self):
        """If baseline median is 0 (dead coin) → fail."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        # All zero volume → baseline median = 0
        candles = _candles(9, volume=0.0)
        assert not d.check_volume_pattern(candles)

    def test_smooth_max_ratio_violation_fails(self):
        """A single spike among sustain bars fails smoothness check."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      smooth_max_ratio=2.0)
        # sustain: [1000, 1000, 10000, 1000] — max/median = 10000/1000 = 10 > 2
        candles = _candles(9, volume=[100] * 5 + [1000, 1000, 10000, 1000])
        assert not d.check_volume_pattern(candles)

    def test_smooth_max_ratio_within_limit_passes(self):
        """A moderate spike within smooth_max_ratio passes."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      smooth_max_ratio=5.0)
        # sustain: [1000, 1000, 3000, 1000] — max/median = 3000/1000 = 3 <= 5
        # last/prev_avg = 1000/(7000/3) ≈ 0.43 < 0.7 → need to avoid decline filter
        candles = _candles(9, volume=[100] * 5 + [1000, 1000, 1000, 3000])
        assert d.check_volume_pattern(candles)

    def test_dump_volume_filter_blocks_last_bar_spike(self):
        """Last bar volume much higher than other sustain bars → dump filter blocks."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      dump_volume_mult=3.0)
        # sustain: [500, 500, 500, 5000] — last/others_median = 5000/500 = 10 > 3
        candles = _candles(9, volume=[100] * 5 + [500, 500, 500, 5000])
        assert not d.check_volume_pattern(candles)

    def test_dump_volume_filter_disabled(self):
        """dump_volume_mult=0 disables the filter."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      dump_volume_mult=0.0, smooth_max_ratio=20.0)
        candles = _candles(9, volume=[100] * 5 + [500, 500, 500, 5000])
        assert d.check_volume_pattern(candles)

    def test_min_baseline_volume_usdt_blocks_low_liquidity(self):
        """Coins with too little volume in USD are filtered out."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      min_baseline_volume_usdt=5000.0)
        # baseline median vol = 100, close ≈ 1.0 → USDT vol = 100 < 5000
        candles = _candles(9, volume=[100] * 5 + [500] * 4)
        assert not d.check_volume_pattern(candles)

    def test_min_baseline_volume_usdt_passes_sufficient_liquidity(self):
        """Coins with enough USD volume pass."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0,
                      min_baseline_volume_usdt=5000.0)
        # baseline median vol = 10000, close ≈ 1.0 → USDT vol = 10000 > 5000
        candles = _candles(9, volume=[10000] * 5 + [50000] * 4)
        assert d.check_volume_pattern(candles)

    def test_regime_multiplier_increases_threshold(self):
        """CAUTIOUS regime raises effective volume threshold."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        d.apply_regime_multiplier(1.5)  # ×1.5 in cautious mode

        candles = _candles(9, volume=[100] * 5 + [400] * 4)
        # baseline median=100, threshold = 100 * 3.0 * 1.5 = 450
        # sustain bars = 400 each < 450 → fail
        assert not d.check_volume_pattern(candles)

        # With 600 each: avg_surge=6.0, min_avg=4.5*1.2=5.4, 6.0>=5.4 → pass
        candles2 = _candles(9, volume=[100] * 5 + [600] * 4)
        assert d.check_volume_pattern(candles2)


# ---------------------------------------------------------------------------
# Price trend
# ---------------------------------------------------------------------------


class TestPriceTrend:
    """check_price_trend — price growth, exhaustion, max growth, ragpull protection."""

    def test_sufficient_growth_returns_long(self):
        """Price grew ≥ price_growth_min_pct over sustain window → long."""
        d = _detector(sustain_bars=4, price_growth_min_pct=1.0)
        # Open goes 1.00 → close 1.02 = +2.0%
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-4]["close"] = 1.005
        candles[-3]["open"] = 1.005
        candles[-3]["close"] = 1.01
        candles[-2]["open"] = 1.01
        candles[-2]["close"] = 1.015
        candles[-1]["open"] = 1.015
        candles[-1]["close"] = 1.02
        for c in candles[-4:]:
            c["high"] = c["close"] * 1.001
            c["low"] = c["open"] * 0.999
        assert d.check_price_trend(candles) == "long"

    def test_insufficient_growth_returns_none(self):
        """Price growth below min → None."""
        d = _detector(sustain_bars=4, price_growth_min_pct=1.0)
        candles = _candles(74, price=1.0)
        # Last 4: open 1.00 → close 1.005 = +0.5%
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 1.005
        for c in candles[-4:]:
            c["high"] = max(c["open"], c["close"]) * 1.001
            c["low"] = min(c["open"], c["close"]) * 0.999
        assert d.check_price_trend(candles) is None

    def test_negative_growth_returns_none(self):
        """Price dropped → None (only long signals)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=1.0)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.02
        candles[-1]["close"] = 1.00
        for c in candles[-4:]:
            c["high"] = max(c["open"], c["close"]) * 1.001
            c["low"] = min(c["open"], c["close"]) * 0.999
        assert d.check_price_trend(candles) is None

    def test_zero_open_returns_none(self):
        """Zero open price → None (malformed data)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=1.0)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 0.0  # malformed
        assert d.check_price_trend(candles) is None

    def test_exhaustion_filter_blocks(self):
        """Growth > exhaustion_gain AND candle closed near high → blocked."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=5.0, exhaustion_pos_ratio=0.7)
        candles = _candles(74, price=1.0)
        # 8% growth in sustain window
        candles[-4]["open"] = 1.00
        candles[-4]["high"] = 1.005
        candles[-4]["low"] = 0.999
        candles[-4]["close"] = 1.002
        candles[-3]["open"] = 1.002
        candles[-3]["close"] = 1.04
        candles[-2]["open"] = 1.04
        candles[-2]["close"] = 1.07
        # Last candle: high=1.085, low=1.06, close=1.08 (near high)
        candles[-1]["open"] = 1.07
        candles[-1]["high"] = 1.085
        candles[-1]["low"] = 1.06
        candles[-1]["close"] = 1.08
        # close_pos = (1.08 - 1.06) / (1.085 - 1.06) = 0.02/0.025 = 0.8 > 0.7
        assert d.check_price_trend(candles) is None

    def test_exhaustion_filter_passes_pullback(self):
        """Growth > exhaustion_gain but candle closed mid-range → passes (pullback)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=5.0, exhaustion_pos_ratio=0.7)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-4]["high"] = 1.005
        candles[-4]["low"] = 0.999
        candles[-4]["close"] = 1.002
        candles[-3]["open"] = 1.002
        candles[-3]["close"] = 1.04
        candles[-2]["open"] = 1.04
        candles[-2]["close"] = 1.07
        # Last candle: high=1.085, low=1.06, close=1.065 (mid-range = pullback)
        candles[-1]["open"] = 1.07
        candles[-1]["high"] = 1.085
        candles[-1]["low"] = 1.06
        candles[-1]["close"] = 1.065
        # close_pos = (1.065 - 1.06) / (1.085 - 1.06) = 0.005/0.025 = 0.2 < 0.7
        assert d.check_price_trend(candles) == "long"

    def test_max_growth_cap_blocks(self):
        """Growth > price_growth_max_pct → blocked (extreme pump already happened)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      price_growth_max_pct=12.0)
        candles = _candles(74, price=1.0)
        # 15% growth
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 1.15
        for c in candles[-4:]:
            c["high"] = max(c["open"], c["close"]) * 1.001
            c["low"] = min(c["open"], c["close"]) * 0.999
        assert d.check_price_trend(candles) is None

    def test_max_growth_cap_disabled(self):
        """price_growth_max_pct=0 disables the cap (exhaustion also off)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      price_growth_max_pct=0.0, exhaustion_gain_pct=0.0)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 2.00  # 100% growth
        for c in candles[-4:]:
            c["high"] = max(c["open"], c["close"]) * 1.001
            c["low"] = min(c["open"], c["close"]) * 0.999
        assert d.check_price_trend(candles) == "long"

    def test_ragpull_protection_blocks(self):
        """Drop > max_hourly_drop_pct during last hour → blocked."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      max_hourly_drop_pct=10.0)
        candles = _candles(74, price=1.0)
        # Normal growth in sustain window
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 1.02
        for c in candles[-4:]:
            c["high"] = c["close"] * 1.001
            c["low"] = c["open"] * 0.999
        # But 30 minutes ago there was a huge drop to 0.85 (15% below current)
        # hour_bars = 60/3 = 20
        candles[-10]["low"] = 0.85
        candles[-10]["close"] = 0.85
        candles[-10]["open"] = 1.0
        candles[-10]["high"] = 1.0
        # ref_price = all_closes[-20] (20 bars ago), recent_low = min of last 20
        # drop = (recent_low / ref_price - 1) * 100
        # With ref_price=1.0 and recent_low=0.85, drop = -15% < -10% → blocked
        assert d.check_price_trend(candles) is None

    def test_ragpull_protection_passes_moderate_drop(self):
        """Drop within allowed range → passes."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      max_hourly_drop_pct=10.0)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 1.02
        for c in candles[-4:]:
            c["low"] = min(c["open"], c["close"]) * 0.999
            c["high"] = max(c["open"], c["close"]) * 1.001
        # -5% drop → ok
        candles[-10]["low"] = 0.95
        candles[-10]["close"] = 0.95
        candles[-10]["open"] = 1.0
        candles[-10]["high"] = 1.0
        assert d.check_price_trend(candles) == "long"

    def test_ragpull_protection_disabled(self):
        """max_hourly_drop_pct=0 disables ragpull protection."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      max_hourly_drop_pct=0.0)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-1]["close"] = 1.02
        for c in candles[-4:]:
            c["high"] = c["close"] * 1.001
            c["low"] = c["open"] * 0.999
        # -50% drop — but protection is off
        candles[-10]["low"] = 0.50
        candles[-10]["close"] = 0.50
        candles[-10]["open"] = 1.0
        candles[-10]["high"] = 1.0
        assert d.check_price_trend(candles) == "long"


# ---------------------------------------------------------------------------
# Exhaustion filter v2 — extreme pump from baseline
# ---------------------------------------------------------------------------


class TestExhaustionV2:
    """Exhaustion v2: блокирует экстремальный памп от baseline (независимо от close_pos)."""

    def test_extreme_pump_from_baseline_blocks(self):
        """Price spiked 40% above baseline median → blocked even with low close_pos."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=5.0,  # extreme threshold = 5 * 6 = 30%
            exhaustion_pos_ratio=0.7,
        )
        # 74 candles: baseline 70 at price ≈ 1.0, sustain 4 at price ≈ 1.02
        # But max high in sustain window spiked to 1.45 (45% above baseline)
        candles = _candles(74, price=1.0)
        # Last 4 candles: normal growth but with a spike candle
        candles[-4]["open"] = 1.00
        candles[-4]["close"] = 1.005
        candles[-4]["high"] = 1.45  # extreme spike!
        candles[-4]["low"] = 0.999
        candles[-3]["open"] = 1.02
        candles[-3]["close"] = 1.025
        candles[-3]["high"] = 1.03
        candles[-3]["low"] = 1.01
        candles[-2]["open"] = 1.025
        candles[-2]["close"] = 1.02
        candles[-2]["high"] = 1.03
        candles[-2]["low"] = 1.01
        # Last candle: closed LOW (dump started), v1 would miss
        candles[-1]["open"] = 1.02
        candles[-1]["high"] = 1.025
        candles[-1]["low"] = 0.995
        candles[-1]["close"] = 0.998  # close_pos = (0.998-0.995)/(1.025-0.995) = 0.1 < 0.7
        assert d.check_price_trend(candles) is None

    def test_moderate_pump_from_baseline_passes(self):
        """Price spiked only 15% above baseline → passes (below 30% extreme threshold)."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=5.0,  # extreme threshold = 30%
            exhaustion_pos_ratio=0.7,
        )
        candles = _candles(74, price=1.0)
        # Max high = 1.15 (15% above baseline median) → below 30% threshold
        candles[-4]["open"] = 1.00
        candles[-4]["close"] = 1.005
        candles[-4]["high"] = 1.15
        candles[-4]["low"] = 0.999
        candles[-3]["open"] = 1.005
        candles[-3]["close"] = 1.01
        candles[-3]["high"] = 1.02
        candles[-3]["low"] = 1.00
        candles[-2]["open"] = 1.01
        candles[-2]["close"] = 1.015
        candles[-2]["high"] = 1.02
        candles[-2]["low"] = 1.005
        candles[-1]["open"] = 1.015
        candles[-1]["close"] = 1.02
        candles[-1]["high"] = 1.025
        candles[-1]["low"] = 1.01
        # change_pct over sustain = (1.02 / 1.00 - 1) * 100 = 2% (< 5% exhaustion v1)
        # extreme_pump = (1.15 / 1.0 - 1) * 100 = 15% (< 30% v2)
        assert d.check_price_trend(candles) == "long"

    def test_extreme_pump_disabled_when_exhaustion_gain_zero(self):
        """exhaustion_gain_pct=0 → v2 also disabled (guarded by ex_gain > 0)."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=0.0,  # disabled
            exhaustion_pos_ratio=0.7,
        )
        candles = _candles(74, price=1.0)
        # 100% spike from baseline
        candles[-4]["open"] = 1.00
        candles[-4]["close"] = 1.005
        candles[-4]["high"] = 2.00
        candles[-4]["low"] = 0.999
        candles[-3]["open"] = 1.005
        candles[-3]["close"] = 1.01
        candles[-3]["high"] = 1.02
        candles[-3]["low"] = 1.00
        candles[-2]["open"] = 1.01
        candles[-2]["close"] = 1.015
        candles[-2]["high"] = 1.02
        candles[-2]["low"] = 1.005
        candles[-1]["open"] = 1.015
        candles[-1]["close"] = 1.02
        candles[-1]["high"] = 1.025
        candles[-1]["low"] = 1.01
        assert d.check_price_trend(candles) == "long"

    def test_pump_and_dump_before_signal_caught(self):
        """Классический pump-and-dump: памп внутри sustain, дамп до сигнала,
        last candle close_pos низкий. v1 пропускает, v2 ловит."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=5.0,  # extreme threshold = 30%
            exhaustion_pos_ratio=0.7,
        )
        candles = _candles(74, price=1.0)
        # Имитация POPCAT-подобного сценария но с более сильным пампом
        # baseline median ≈ 1.0
        # Candle -4: PUMP, high=1.50 (+50% от baseline)
        candles[-4]["open"] = 1.00
        candles[-4]["high"] = 1.50
        candles[-4]["low"] = 0.99
        candles[-4]["close"] = 1.45
        # Candle -3: peak continuation
        candles[-3]["open"] = 1.45
        candles[-3]["high"] = 1.48
        candles[-3]["low"] = 1.35
        candles[-3]["close"] = 1.38
        # Candle -2: dump starts
        candles[-2]["open"] = 1.38
        candles[-2]["high"] = 1.40
        candles[-2]["low"] = 1.15
        candles[-2]["close"] = 1.18
        # Candle -1 (signal): dump continues, close at bottom
        candles[-1]["open"] = 1.18
        candles[-1]["high"] = 1.20
        candles[-1]["low"] = 1.02
        candles[-1]["close"] = 1.03
        # change_pct over sustain = (1.03 / 1.00 - 1) * 100 = 3% (< 5%, v1 misses)
        # close_pos = (1.03 - 1.02) / (1.20 - 1.02) = 0.056 (< 0.7, v1 misses)
        # extreme_pump = (1.50 / 1.0 - 1) * 100 = 50% (> 30%, v2 catches!)
        assert d.check_price_trend(candles) is None

    def test_extreme_pump_at_threshold_boundary(self):
        """Памп на границе порога: 29% проходит, 31% блокируется."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=5.0,  # extreme threshold = 30%
            exhaustion_pos_ratio=0.7,
        )
        # 29% — проходит (ниже порога)
        candles = _candles(74, price=1.0)
        candles[-4]["open"] = 1.00
        candles[-4]["close"] = 1.005
        candles[-4]["high"] = 1.29
        candles[-4]["low"] = 0.999
        for i in range(-3, 0):
            candles[i]["open"] = 1.01
            candles[i]["close"] = 1.015
            candles[i]["high"] = 1.02
            candles[i]["low"] = 1.00
        assert d.check_price_trend(candles) == "long"

        # 31% — блокируется (выше порога)
        candles[-4]["high"] = 1.31
        assert d.check_price_trend(candles) is None

    def test_v1_and_v2_independent(self):
        """v1 (orderly exhaustion) и v2 (extreme pump) работают независимо:
        v1 может пропустить (pullback), но v2 ловит экстремальный памп."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=5.0,
            exhaustion_pos_ratio=0.7,
        )
        candles = _candles(74, price=1.0)
        # v1 condition: change_pct > 5% (over sustain) → сделаем небольшой рост 6%
        # Но close_pos низкий → v1 НЕ блокирует (pullback)
        candles[-4]["open"] = 1.00
        candles[-4]["high"] = 1.70  # +70% extreme pump — v2 должно сработать
        candles[-4]["low"] = 0.99
        candles[-4]["close"] = 1.01
        candles[-3]["open"] = 1.01
        candles[-3]["close"] = 1.03
        candles[-3]["high"] = 1.04
        candles[-3]["low"] = 1.00
        candles[-2]["open"] = 1.03
        candles[-2]["close"] = 1.05
        candles[-2]["high"] = 1.06
        candles[-2]["low"] = 1.02
        candles[-1]["open"] = 1.05
        candles[-1]["close"] = 1.06
        candles[-1]["high"] = 1.07
        candles[-1]["low"] = 1.04
        # change_pct = (1.06 / 1.00 - 1) * 100 = 6% (> 5%)
        # close_pos = (1.06 - 1.04) / (1.07 - 1.04) = 0.02/0.03 = 0.67 (< 0.7)
        # v1: change_pct > 5% но close_pos < 0.7 → пропускает
        # v2: (1.70 / 1.0 - 1) * 100 = 70% > 30% → БЛОК
        assert d.check_price_trend(candles) is None


# ---------------------------------------------------------------------------
# Exhaustion edge cases (v1)
# ---------------------------------------------------------------------------


class TestExhaustionEdgeCases:
    """Edge cases for the exhaustion filter."""

    def test_exhaustion_disabled_when_gain_zero(self):
        """exhaustion_gain_pct=0 → filter disabled."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, exhaustion_pos_ratio=0.7)
        candles = _candles(74, price=1.0)
        # 8% growth, candle at top
        candles[-4]["open"] = 1.00
        candles[-1]["high"] = 1.085
        candles[-1]["low"] = 1.06
        candles[-1]["close"] = 1.08  # near high
        for c in candles[-4:]:
            if c is not candles[-1]:
                c["high"] = c["close"] * 1.001
                c["low"] = c["open"] * 0.999
        assert d.check_price_trend(candles) == "long"

    def test_exhaustion_not_triggered_below_threshold(self):
        """Growth below exhaustion_gain_pct → filter not checked."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=5.0, exhaustion_pos_ratio=0.7)
        candles = _candles(74, price=1.0)
        # 3% growth (< 5% exhaustion threshold), candle at top
        candles[-4]["open"] = 1.00
        candles[-1]["high"] = 1.04
        candles[-1]["low"] = 1.02
        candles[-1]["close"] = 1.039  # near high
        for c in candles[-4:]:
            if c is not candles[-1]:
                c["high"] = c["close"] * 1.001
                c["low"] = c["open"] * 0.999
        assert d.check_price_trend(candles) == "long"

    def test_zero_range_candle_bypasses_exhaustion(self):
        """Candle with high == low → division by zero avoided, passes."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=5.0, exhaustion_pos_ratio=0.7)
        candles = _candles(74, price=1.0)
        # 10% growth, but last candle has no range (high==low)
        candles[-4]["open"] = 1.00
        candles[-3]["close"] = 1.05
        candles[-2]["close"] = 1.08
        candles[-1]["open"] = 1.08
        candles[-1]["high"] = 1.10
        candles[-1]["low"] = 1.10  # no range
        candles[-1]["close"] = 1.10
        for c in candles[-3:-1]:
            c["high"] = c["close"] * 1.001
            c["low"] = c["open"] * 0.999
        # Should not crash, passes because range=0 → close_pos check skipped
        assert d.check_price_trend(candles) == "long"


# ---------------------------------------------------------------------------
# OI slope calculation
# ---------------------------------------------------------------------------


class TestOISlope:
    """calculate_oi_slope_pct — open interest trend detection."""

    def test_rising_oi_positive_slope(self):
        """Growing OI → positive slope."""
        values = np.array([100.0, 110.0, 120.0])
        slope = calculate_oi_slope_pct(values)
        assert slope is not None
        assert slope > 0

    def test_falling_oi_negative_slope(self):
        """Declining OI → negative slope."""
        values = np.array([120.0, 110.0, 100.0])
        slope = calculate_oi_slope_pct(values)
        assert slope is not None
        assert slope < 0

    def test_flat_oi_zero_slope(self):
        """Flat OI → near-zero slope."""
        values = np.array([100.0, 100.0, 100.0])
        slope = calculate_oi_slope_pct(values)
        assert slope is not None
        assert abs(slope) < 0.01

    def test_insufficient_points_returns_none(self):
        """Less than 2 points → None."""
        assert calculate_oi_slope_pct(np.array([100.0])) is None

    def test_zero_mean_returns_none(self):
        """All zeros → None (mean = 0 → division by zero)."""
        assert calculate_oi_slope_pct(np.array([0.0, 0.0, 0.0])) is None

    def test_large_oi_values(self):
        """Large OI values (millions) still produce valid slope."""
        values = np.array([45_000_000.0, 46_000_000.0, 48_000_000.0])
        slope = calculate_oi_slope_pct(values)
        assert slope is not None
        # slope ≈ 9.7% for these values
        assert 8.0 < slope < 12.0


# ---------------------------------------------------------------------------
# Signal building
# ---------------------------------------------------------------------------


class TestSignalBuilding:
    """_build_signal — confidence calculation and message format."""

    def test_confidence_capped_at_95(self):
        """Confidence is capped at 100 regardless of volume surge."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        # Huge surge: mean(sustain)/baseline = 5000/100 = 50 → confidence = 50*5 = 250 → capped at 100
        candles = _candles(9, volume=[100] * 5 + [5000] * 4)
        sig = d._build_signal("TEST/USDT", "long", candles)
        assert sig.confidence == 100

    def test_confidence_scales_with_surge(self):
        """Confidence = min(round(surge * 5), 100)."""
        d = _detector(baseline_bars=5, sustain_bars=4, volume_surge_mult=3.0)
        # surge = mean([500,500,500,500]) / 100 = 5 → 5*5 = 25
        candles = _candles(9, volume=[100] * 5 + [500] * 4)
        sig = d._build_signal("TEST/USDT", "long", candles)
        assert sig.confidence == 25

    def test_signal_has_required_fields(self):
        """Signal has all fields expected by downstream code."""
        d = _detector()
        candles = _candles(74, volume=100_000)
        sig = d._build_signal("ME/USDT", "long", candles)
        assert sig.symbol == "ME/USDT"
        assert sig.setup_type == "volume_surge"
        assert sig.direction == "long"
        assert 0 <= sig.confidence <= 95
        assert "Объём" in sig.message
        assert "ME/USDT" not in sig.message  # symbol not duplicated in message body


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfigDefaults:
    """Config defaults are sensible."""

    def test_default_strategy_config(self):
        cfg = StrategyConfig()
        assert cfg.baseline_bars == 50
        assert cfg.sustain_bars == 4
        assert cfg.volume_surge_mult == 2.0
        assert cfg.oi_slope_min_pct == 2.0
        assert cfg.price_growth_min_pct == 1.0

    def test_cautious_increase_bounds(self):
        """cautious_volume_surge_mult_increase_pct is within valid range."""
        cfg = StrategyConfig()
        assert 0.0 <= cfg.cautious_volume_surge_mult_increase_pct <= 200.0
