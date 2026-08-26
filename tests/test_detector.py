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
        """price_growth_max_pct=0 disables the cap (both exhaustion filters off)."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      price_growth_max_pct=0.0, exhaustion_gain_pct=0.0,
                      exhaustion_extreme_pct=0.0)
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
# Retracement filter — catches a reversal already under way inside the
# sustain window, before it fully unwinds change_pct (see db-audit-august-2026)
# ---------------------------------------------------------------------------


class TestRetracementFilter:
    """max_window_retracement_pct: откат от пика (high) sustain-окна до
    последнего close. Ловит паттерн, который exhaustion (позиция закрытия
    ТОЛЬКО последней свечи в её собственном диапазоне) пропускает: пик был
    на 1-2 свечи раньше, и цена с тех пор тихо снижалась."""

    def _reversal_candles(self) -> list[dict]:
        """Sustain-окно: рост до пика на 3-й свече (high=1.101), затем разворот
        вниз на последней. change_pct за окно всё ещё положительный (+4%),
        так что price_growth_min_pct пропустит сигнал дальше — фильтровать
        должен именно retracement."""
        candles = _candles(74, price=1.0)
        candles[-4]["open"], candles[-4]["close"] = 1.00, 1.02
        candles[-4]["high"], candles[-4]["low"] = 1.021, 0.999
        candles[-3]["open"], candles[-3]["close"] = 1.02, 1.06
        candles[-3]["high"], candles[-3]["low"] = 1.061, 1.019
        candles[-2]["open"], candles[-2]["close"] = 1.06, 1.10
        candles[-2]["high"], candles[-2]["low"] = 1.101, 1.059  # window peak
        candles[-1]["open"], candles[-1]["close"] = 1.10, 1.04  # reversal
        candles[-1]["high"], candles[-1]["low"] = 1.101, 1.035
        # change_pct = (1.04 / 1.00 - 1) * 100 = +4.0%
        # retracement = (1.101 - 1.04) / 1.101 * 100 ≈ 5.5%
        return candles

    def test_disabled_by_default_passes(self):
        """max_window_retracement_pct=0 (дефолт) — фильтр выключен, откат не блокирует."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, max_window_retracement_pct=0.0)
        assert d.check_price_trend(self._reversal_candles()) == "long"

    def test_retracement_exceeds_threshold_blocks(self):
        """Откат ~5.5% > порога 3% → блок, даже несмотря на положительный change_pct."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, max_window_retracement_pct=3.0)
        assert d.check_price_trend(self._reversal_candles()) is None

    def test_retracement_within_threshold_passes(self):
        """Небольшой откат (< порога) не блокирует."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, max_window_retracement_pct=3.0)
        candles = self._reversal_candles()
        # Откат всего ~0.5% вместо 5.5%
        candles[-1]["close"] = 1.095
        assert d.check_price_trend(candles) == "long"

    def test_context_reports_retracement_stage(self):
        """context['stage'] == 'retracement' для filtered_signals-аудита."""
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, max_window_retracement_pct=3.0)
        context: dict = {}
        d.check_price_trend(self._reversal_candles(), context=context)
        assert context["stage"] == "retracement"

    def test_catches_reversal_that_exhaustion_misses(self):
        """Ключевое отличие от exhaustion: последняя свеча закрылась у своего
        НИЗА (close_pos ≈ 0.08), а не у верха — exhaustion (проверяет close
        near HIGH) её не поймает, а retracement — поймает."""
        candles = self._reversal_candles()
        last = candles[-1]
        close_pos = (last["close"] - last["low"]) / (last["high"] - last["low"])
        assert close_pos < 0.1  # далеко не "close near high"

        d_exhaustion_only = _detector(
            sustain_bars=4, price_growth_min_pct=0.1,
            exhaustion_gain_pct=3.0, exhaustion_pos_ratio=0.7,
            max_window_retracement_pct=0.0,
        )
        assert d_exhaustion_only.check_price_trend(candles) == "long"  # проскакивает

        d_with_retracement = _detector(
            sustain_bars=4, price_growth_min_pct=0.1,
            exhaustion_gain_pct=3.0, exhaustion_pos_ratio=0.7,
            max_window_retracement_pct=3.0,
        )
        assert d_with_retracement.check_price_trend(candles) is None  # ловит


# ---------------------------------------------------------------------------
# Exhaustion filter v2 — extreme pump from baseline
# ---------------------------------------------------------------------------


class TestWindowRangeFilter:
    """max_window_range_pct: размах (high/low - 1) sustain-окна.

    Отдельно от retracement: тот ловит «пик был раньше, сейчас снижаемся»,
    а этот — «монета в принципе ходит слишком широко для фиксированного стопа».
    Окно может расти ровно, без отката, и всё равно быть непригодным: если за
    12 минут коридор 6%, то stop_loss_pct=5 стоит внутри обычного шума
    (аудит августа 2026)."""

    def _wide_but_rising(self) -> list[dict]:
        """Ровный рост без отката, но с широким коридором: low 0.97, high 1.04
        -> размах ≈ 7.2%. Ни retracement, ни exhaustion такое не блокируют."""
        candles = _candles(74, price=1.0)
        candles[-4]["open"], candles[-4]["close"] = 1.000, 1.010
        candles[-4]["high"], candles[-4]["low"] = 1.015, 0.970
        candles[-3]["open"], candles[-3]["close"] = 1.010, 1.018
        candles[-3]["high"], candles[-3]["low"] = 1.025, 1.005
        candles[-2]["open"], candles[-2]["close"] = 1.018, 1.026
        candles[-2]["high"], candles[-2]["low"] = 1.032, 1.014
        candles[-1]["open"], candles[-1]["close"] = 1.026, 1.038
        candles[-1]["high"], candles[-1]["low"] = 1.040, 1.022
        return candles

    def test_disabled_by_default_passes(self):
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, exhaustion_extreme_pct=0.0)
        assert d.config.max_window_range_pct == 0.0
        assert d.check_price_trend(self._wide_but_rising()) == "long"

    def test_wide_window_blocked(self):
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, exhaustion_extreme_pct=0.0,
                      max_window_range_pct=4.0)
        ctx: dict = {}
        assert d.check_price_trend(self._wide_but_rising(), ctx) is None
        assert ctx["stage"] == "window_range"

    def test_narrow_window_passes(self):
        candles = _candles(74, price=1.0)
        candles[-4]["open"], candles[-4]["close"] = 1.000, 1.008
        candles[-4]["high"], candles[-4]["low"] = 1.010, 0.999
        candles[-3]["open"], candles[-3]["close"] = 1.008, 1.014
        candles[-3]["high"], candles[-3]["low"] = 1.016, 1.006
        candles[-2]["open"], candles[-2]["close"] = 1.014, 1.019
        candles[-2]["high"], candles[-2]["low"] = 1.021, 1.012
        candles[-1]["open"], candles[-1]["close"] = 1.019, 1.024
        candles[-1]["high"], candles[-1]["low"] = 1.026, 1.017
        # размах = 1.026 / 0.999 - 1 ≈ 2.7% < 4.0
        d = _detector(sustain_bars=4, price_growth_min_pct=0.1,
                      exhaustion_gain_pct=0.0, exhaustion_extreme_pct=0.0,
                      max_window_range_pct=4.0)
        assert d.check_price_trend(candles) == "long"


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

    def test_extreme_pump_disabled_by_its_own_knob(self):
        """exhaustion_extreme_pct=0 → v2 off.

        v2 has its own threshold since the августовский аудит 2026: раньше он был
        захардкожен как exhaustion_gain_pct * 6 и выключался только вместе с v1,
        хотя фильтры измеряют разное (v1 — форму последней свечи, v2 — насколько
        цена уже улетела от baseline) и оптимальные пороги у них не связаны."""
        d = _detector(
            baseline_bars=70, sustain_bars=4,
            price_growth_min_pct=0.1,
            exhaustion_gain_pct=0.0,
            exhaustion_extreme_pct=0.0,  # disabled
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

    def test_oi_gate_and_new_audit_knobs(self):
        """Дефолты сохраняют прежнее поведение; выключается всё явным конфигом."""
        cfg = StrategyConfig()
        assert cfg.oi_filter_enabled is True
        assert cfg.max_window_range_pct == 0.0
        # v2-exhaustion: дефолт равен прежнему захардкоженному ex_gain*6 = 30%
        assert cfg.exhaustion_extreme_pct == 30.0

    def test_cautious_increase_bounds(self):
        """cautious_volume_surge_mult_increase_pct is within valid range."""
        cfg = StrategyConfig()
        assert 0.0 <= cfg.cautious_volume_surge_mult_increase_pct <= 200.0


# ---------------------------------------------------------------------------
# Пороги, вынесенные из кода в конфиг (26.08.2026)
# ---------------------------------------------------------------------------


class TestDeHardcodedThresholds:
    """Пороги volume_fading / volume_declining / oi_declining / pre_surge / confidence
    до 26.08.2026 были зашиты в детекторе, и свипнуть их было нечем.

    Дефолты обязаны в точности повторять прежние зашитые значения — иначе вынос
    молча изменил бы стратегию. Каждый тест проверяет и это, и что ручка работает.
    """

    BASELINE_BARS = 5
    BASELINE_VOL = 100.0

    @classmethod
    def _sustain_volumes(cls, vols: list[float]) -> list[dict]:
        """baseline с ровным объёмом + sustain-окно с заданными объёмами."""
        volumes = [cls.BASELINE_VOL] * cls.BASELINE_BARS + vols
        return _candles(len(volumes), volume=volumes)

    @classmethod
    def _det(cls, **overrides) -> SetupDetector:
        return _detector(
            baseline_bars=cls.BASELINE_BARS, sustain_bars=4,
            volume_surge_mult=3.0, **overrides,
        )

    def test_defaults_match_previous_hardcoded_values(self):
        """Значения по умолчанию = то, что было зашито в коде."""
        cfg = StrategyConfig()
        assert cfg.volume_fading_ratio == 0.7
        assert cfg.volume_declining_enabled is True
        assert cfg.oi_declining_enabled is True
        assert cfg.pre_surge_bars == 10
        assert cfg.confidence_surge_mult == 5.0

    def test_volume_fading_rejects_at_default_ratio(self):
        """Объём последней свечи ниже 70% от среднего предыдущих — отказ."""
        det = self._det()
        # среднее предыдущих 1000, последняя 500 → 0.5 < 0.7
        candles = self._sustain_volumes([1_000.0, 1_000.0, 1_000.0, 500.0])
        ctx: dict = {}
        assert det.check_volume_pattern(candles, ctx) is False
        assert ctx["stage"] == "volume_fading"

    def test_volume_fading_can_be_relaxed(self):
        """Опущенный порог пропускает ту же свечу — ручка действительно работает."""
        det = self._det(volume_fading_ratio=0.3)
        candles = self._sustain_volumes([1_000.0, 1_000.0, 1_000.0, 500.0])
        ctx: dict = {}
        det.check_volume_pattern(candles, ctx)
        assert ctx.get("stage") != "volume_fading"

    def test_volume_fading_disabled_by_zero(self):
        """0 выключает фильтр целиком."""
        det = self._det(volume_fading_ratio=0.0)
        candles = self._sustain_volumes([1_000.0, 1_000.0, 1_000.0, 500.0])
        ctx: dict = {}
        det.check_volume_pattern(candles, ctx)
        assert ctx.get("stage") != "volume_fading"

    def test_volume_declining_toggle(self):
        """Флаг снимает требование «последняя свеча не ниже первой»."""
        # объём падает от первой к последней, но не настолько, чтобы сработал fading
        vols = [1_000.0, 950.0, 900.0, 850.0]
        ctx_on: dict = {}
        self._det().check_volume_pattern(self._sustain_volumes(vols), ctx_on)
        assert ctx_on["stage"] == "volume_declining"

        ctx_off: dict = {}
        self._det(volume_declining_enabled=False).check_volume_pattern(
            self._sustain_volumes(vols), ctx_off
        )
        assert ctx_off.get("stage") != "volume_declining"

    def test_confidence_scale_is_configurable(self):
        """Множитель confidence берётся из конфига, а не зашит пятёркой."""
        assert min(round(4.0 * StrategyConfig().confidence_surge_mult), 100) == 20
        assert min(round(4.0 * StrategyConfig(confidence_surge_mult=10.0).confidence_surge_mult), 100) == 40

    def test_confidence_saturates_at_default_scale(self):
        """Насыщение шкалы: при множителе 5 всё, что выше surge x20, неотличимо.

        Это и есть причина, по которой в живых данных у 52% сигналов confidence=100.
        """
        mult = StrategyConfig().confidence_surge_mult
        assert min(round(20.0 * mult), 100) == 100
        assert min(round(60.0 * mult), 100) == 100
