"""
Тесты MarketContext — того, что решает, торговать ли вообще.

549 строк, из них Supertrend, определение режима и тренда — чистая математика,
и до 26.08.2026 всё это проверялось только на живом рынке. При этом `risk_off`
запрещает входы по всем сигналам, а `cautious` вдвое режет размер позиции, так
что ошибка здесь дороже ошибки в любом отдельном фильтре детектора.

Отдельно зафиксировано поведение при сбое биржи: `_calc_btc_changes` обязан
удерживать последнее известное значение, а не откатываться к нулю. Раньше здесь
стоял фолбэк на историю тикеров, который не мог сработать никогда (BTC входит в
exclude_coins, строк BTC в `tickers` не было), и любой сбой сети молча означал
«BTC стоит на месте» — то есть `risk_off` не наступал.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.analytics.market_context import MarketContext
from src.config import MarketContextConfig


def _ctx(**config_overrides) -> MarketContext:
    params = {
        "enabled": True,
        "btc_drop_threshold_pct": 1.5,
        "trend_threshold_pct": 1.0,
        "supertrend_atr_period": 10,
        "supertrend_multiplier": 3.0,
    }
    params.update(config_overrides)
    return MarketContext(MarketContextConfig(**params), connector=MagicMock())


def _bars(closes: list[float], spread: float = 0.01) -> list[dict]:
    """Бары вокруг заданной траектории закрытий."""
    return [
        {
            "open": c,
            "high": c * (1 + spread),
            "low": c * (1 - spread),
            "close": c,
        }
        for c in closes
    ]


# ---------------------------------------------------------------------------
# Режим — гейт входов
# ---------------------------------------------------------------------------


class TestRegime:
    """risk_off / cautious / risk_on из падения BTC за час и цвета Supertrend."""

    @staticmethod
    def _regime(ctx: MarketContext, btc_1h: float, st_color: str) -> str:
        ctx._btc_change_1h = btc_1h
        ctx._supertrend_color = st_color
        return ctx._determine_regime()

    def test_btc_drop_and_red_is_risk_off(self):
        """Падает BTC и Supertrend красный — худшее сочетание, входы запрещены."""
        assert self._regime(_ctx(), -2.0, "red") == "risk_off"

    def test_btc_drop_alone_is_cautious(self):
        assert self._regime(_ctx(), -2.0, "green") == "cautious"

    def test_red_alone_is_cautious(self):
        assert self._regime(_ctx(), 0.5, "red") == "cautious"

    def test_calm_and_green_is_risk_on(self):
        assert self._regime(_ctx(), 0.5, "green") == "risk_on"

    def test_threshold_is_exclusive(self):
        """Ровно на пороге — ещё не падение (btc_bearish строго меньше -порога)."""
        ctx = _ctx(btc_drop_threshold_pct=1.5)
        assert self._regime(ctx, -1.5, "green") == "risk_on"
        assert self._regime(ctx, -1.51, "green") == "cautious"

    def test_threshold_is_configurable(self):
        assert self._regime(_ctx(btc_drop_threshold_pct=0.5), -1.0, "green") == "cautious"
        assert self._regime(_ctx(btc_drop_threshold_pct=5.0), -1.0, "green") == "risk_on"


class TestBlockEntriesAndSizing:
    """Что режим делает с входами и размером позиции."""

    @staticmethod
    def _ready(regime: str, st_color: str) -> MarketContext:
        ctx = _ctx()
        ctx._ready = True
        ctx._regime = regime
        ctx._supertrend_color = st_color
        return ctx

    def test_risk_off_blocks(self):
        assert self._ready("risk_off", "green").should_block_entries() is True

    def test_cautious_red_blocks(self):
        """Аудит июня 2026: 5/5 сделок в этом сочетании убыточны."""
        assert self._ready("cautious", "red").should_block_entries() is True

    def test_cautious_green_allows(self):
        assert self._ready("cautious", "green").should_block_entries() is False

    def test_risk_on_allows_even_when_red(self):
        assert self._ready("risk_on", "red").should_block_entries() is False

    def test_not_ready_never_blocks(self):
        """Без данных не блокируем — иначе бот молча встанет на старте."""
        ctx = _ctx()
        ctx._ready = False
        ctx._regime = "risk_off"
        assert ctx.should_block_entries() is False

    def test_cautious_halves_position_size(self):
        assert self._ready("cautious", "green").position_size_multiplier() == 0.5

    def test_risk_on_keeps_full_size(self):
        assert self._ready("risk_on", "green").position_size_multiplier() == 1.0


# ---------------------------------------------------------------------------
# Тренд
# ---------------------------------------------------------------------------


class TestTrend:
    """bullish/bearish требуют согласия всех трёх сигналов; иначе neutral."""

    @staticmethod
    def _trend(others_4h: float, btc_4h: float, st_color: str, threshold: float = 1.0) -> str:
        ctx = _ctx(trend_threshold_pct=threshold)
        ctx._others_change_4h = others_4h
        ctx._btc_change_4h = btc_4h
        ctx._supertrend_color = st_color
        return ctx._determine_trend()

    def test_all_three_up_is_bullish(self):
        assert self._trend(2.0, 2.0, "green") == "bullish"

    def test_all_three_down_is_bearish(self):
        assert self._trend(-2.0, -2.0, "red") == "bearish"

    def test_disagreement_is_neutral(self):
        assert self._trend(2.0, 2.0, "red") == "neutral"       # ST против
        assert self._trend(2.0, -2.0, "green") == "neutral"    # BTC против
        assert self._trend(-2.0, 2.0, "red") == "neutral"      # OTHERS против

    def test_inside_threshold_is_neutral(self):
        assert self._trend(0.5, 0.5, "green") == "neutral"


# ---------------------------------------------------------------------------
# Supertrend
# ---------------------------------------------------------------------------


class TestSupertrend:
    """Индикатор, от цвета которого зависит и режим, и тренд."""

    def test_not_enough_bars_leaves_color_untouched(self):
        """Меньше period+1 баров — цвет не трогаем, а не выставляем наугад."""
        ctx = _ctx(supertrend_atr_period=10)
        ctx._supertrend_color = "green"
        ctx._bars = _bars([100.0] * 5)
        ctx._compute_supertrend()
        assert ctx._supertrend_color == "green"

    def test_steady_uptrend_is_green(self):
        ctx = _ctx(supertrend_atr_period=5, supertrend_multiplier=2.0)
        ctx._bars = _bars([100.0 * (1.02 ** i) for i in range(40)])
        ctx._compute_supertrend()
        assert ctx._supertrend_color == "green"

    def test_steady_downtrend_is_red(self):
        ctx = _ctx(supertrend_atr_period=5, supertrend_multiplier=2.0)
        ctx._bars = _bars([100.0 * (0.98 ** i) for i in range(40)])
        ctx._compute_supertrend()
        assert ctx._supertrend_color == "red"

    def test_reversal_flips_color(self):
        """Рост, затем разворот вниз — цвет обязан смениться на red."""
        ctx = _ctx(supertrend_atr_period=5, supertrend_multiplier=2.0)
        up = [100.0 * (1.02 ** i) for i in range(30)]
        down = [up[-1] * (0.96 ** i) for i in range(1, 25)]
        ctx._bars = _bars(up + down)
        ctx._compute_supertrend()
        assert ctx._supertrend_color == "red"


# ---------------------------------------------------------------------------
# BTC: поведение при сбое биржи
# ---------------------------------------------------------------------------


class TestBtcChanges:
    """Сбой не должен молча превращаться в «BTC стоит на месте»."""

    @staticmethod
    def _candles(closes: list[float]) -> list[dict]:
        return [{"close": c} for c in closes]

    async def test_computes_from_hourly_candles(self):
        ctx = _ctx()
        ctx._connector.fetch_ohlcv = AsyncMock(
            return_value=self._candles([100.0, 101.0, 102.0, 103.0, 110.0])
        )
        change_1h, change_4h = await ctx._calc_btc_changes()
        assert change_1h == pytest.approx((110.0 / 103.0 - 1) * 100)
        assert change_4h == pytest.approx((110.0 / 100.0 - 1) * 100)

    async def test_keeps_last_known_values_on_error(self):
        """Ключевая регрессия: раньше сбой давал (0.0, 0.0) и отменял risk_off."""
        ctx = _ctx()
        ctx._btc_change_1h = -3.0
        ctx._btc_change_4h = -7.0
        ctx._connector.fetch_ohlcv = AsyncMock(side_effect=TimeoutError("exchange down"))

        assert await ctx._calc_btc_changes() == (-3.0, -7.0)

        # И режим по-прежнему считается как risk_off, а не как «рынок спокоен»
        ctx._supertrend_color = "red"
        assert ctx._determine_regime() == "risk_off"

    async def test_keeps_last_known_values_on_short_response(self):
        ctx = _ctx()
        ctx._btc_change_1h = -3.0
        ctx._btc_change_4h = -7.0
        ctx._connector.fetch_ohlcv = AsyncMock(return_value=self._candles([100.0]))
        assert await ctx._calc_btc_changes() == (-3.0, -7.0)

    async def test_partial_response_keeps_only_missing_horizon(self):
        """Свечей хватает на 1ч, но не на 4ч — обновляем то, что можно."""
        ctx = _ctx()
        ctx._btc_change_4h = -7.0
        ctx._connector.fetch_ohlcv = AsyncMock(return_value=self._candles([100.0, 105.0]))
        change_1h, change_4h = await ctx._calc_btc_changes()
        assert change_1h == pytest.approx(5.0)
        assert change_4h == -7.0
