"""ATR-стоп (13.10.2026): пересмотр июньского отказа из docs/decisions.md.

Проверяются три слоя одной механики:
- формулы `analytics.utils` (atr_stop_pct / resolve_stop_pct / масштаб трейлинга)
  — единственная реализация на прод и движок (правило 2 AGENTS.md);
- мера `SetupDetector.atr_pct` — baseline (до sustain-окна) против signal
  (включая памп-бары) и её прокладка в контекст/Signal;
- поведение движка и PositionManager: пер-сделочный стоп, размер позиции,
  масштабированный трейлинг, фолбэк для старых сделок без stop_pct.

Мутационные проверки описаны в докстрингах тестов.
"""

import sqlite3
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.analytics.detector import SetupDetector
from src.analytics.utils import (
    atr_stop_pct,
    resolve_stop_pct,
    trailing_activation_price,
    trailing_distance,
    trailing_scale,
)
from src.backtest.engine import load_data, simulate
from src.config import CollectorsConfig, Settings, StrategyConfig, TradingConfig

# ---------------------------------------------------------------------------
# Формулы (utils)
# ---------------------------------------------------------------------------


class TestAtrStopPct:
    def test_off_returns_fixed(self):
        """mult=0 — фиксированный стоп: поведение до 13.10.2026, основание parity."""
        assert atr_stop_pct(0.5, 0.0, 4.0) == 4.0

    def test_missing_measure_returns_fixed(self):
        """Нет ATR (короткое окно) — фиксированный стоп, а не отказ от сделки."""
        assert atr_stop_pct(None, 6.0, 4.0) == 4.0
        assert atr_stop_pct(0.0, 6.0, 4.0) == 4.0

    def test_product(self):
        assert atr_stop_pct(0.5, 6.0, 4.0) == pytest.approx(3.0)

    def test_floor_two_pct(self):
        """Ниже 2% стоп сидит в комиссии/проскальзывании (круговая ≈ 0.11% в цене)."""
        assert atr_stop_pct(0.1, 6.0, 4.0) == 2.0

    def test_cap_is_fixed_stop(self):
        """ATR умеет только СУЖАТЬ: шире боевого стопа не бывает.

        Мутация: cap на ADAPTIVE_STOP_MAX_PCT (10%) — тест падает на 6.0 > 4.0.
        """
        assert atr_stop_pct(1.0, 6.0, 4.0) == 4.0


class TestResolveStopPct:
    def _cfg(self, **kw) -> TradingConfig:
        params = {"stop_loss_pct": 4.0, "stop_loss_atr_mult": 0.0,
                  "stop_loss_window_range_mult": 0.0}
        params.update(kw)
        return TradingConfig(**params)

    def test_all_off(self):
        assert resolve_stop_pct(2.0, 0.4, self._cfg()) == 4.0

    def test_atr_only(self):
        cfg = self._cfg(stop_loss_atr_mult=6.0)
        assert resolve_stop_pct(None, 0.5, cfg) == pytest.approx(3.0)
        assert resolve_stop_pct(2.0, None, cfg) == 4.0

    def test_window_range_only(self):
        cfg = self._cfg(stop_loss_window_range_mult=1.0)
        assert resolve_stop_pct(3.0, None, cfg) == pytest.approx(3.0)


class TestTrailingScale:
    TRAIL = {"trailing_activation_pct": 8.0, "trailing_distance_pct": 5.0,
             "stop_loss_pct": 4.0}

    def test_off_is_bitwise_old_formula(self):
        """Флаг выключен — значения в точности прежние (parity с 11.10.2026)."""
        cfg = TradingConfig(**self.TRAIL)
        assert trailing_scale(cfg, 2.0) == 1.0
        assert trailing_activation_price(100.0, cfg, 2.0) == pytest.approx(108.0)
        assert trailing_distance(100.0, cfg, 2.0) == pytest.approx(5.0)

    def test_no_stop_no_scale(self):
        cfg = TradingConfig(**self.TRAIL, trailing_scale_with_stop=True)
        assert trailing_scale(cfg, None) == 1.0

    def test_scaled_keeps_stop_proportions(self):
        """Стоп 2% (вдвое меньше боевого 4%) — геометрия сжимается вдвое.

        При боевых пропорциях активация = 2× стопа, отступ = 1.25× стопа:
        масштабированный отступ всегда ровно стоп-дистанция. Мутация: убрать
        умножение на scale в trailing_distance — равенство ломается.
        """
        cfg = TradingConfig(**self.TRAIL, trailing_scale_with_stop=True)
        assert trailing_scale(cfg, 2.0) == pytest.approx(0.5)
        assert trailing_activation_price(100.0, cfg, 2.0) == pytest.approx(104.0)
        assert trailing_distance(100.0, cfg, 2.0) == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------


class TestAtrConfig:
    def test_adaptive_stops_are_exclusive(self):
        with pytest.raises(Exception, match="взаимоисключающие"):
            TradingConfig(stop_loss_pct=4.0, stop_loss_window_range_mult=1.0,
                          stop_loss_atr_mult=6.0)

    def test_unknown_source_rejected(self):
        with pytest.raises(Exception, match="baseline"):
            TradingConfig(stop_loss_pct=4.0, stop_loss_atr_mult=6.0,
                          stop_loss_atr_source="pump")

    def test_defaults_are_off(self):
        """Дефолты не меняют поведение: это основа parity-прогона по правилу 3."""
        cfg = TradingConfig()
        assert cfg.stop_loss_atr_mult == 0.0
        assert cfg.trailing_scale_with_stop is False
        assert cfg.stop_loss_atr_source == "baseline"


# ---------------------------------------------------------------------------
# Мера: SetupDetector.atr_pct
# ---------------------------------------------------------------------------

STRATEGY_PARAMS = dict(
    exclude_coins=[], baseline_bars=20, volume_surge_mult=5.0, sustain_bars=4,
    oi_filter_enabled=False, price_growth_min_pct=1.0, price_growth_max_pct=25.0,
    min_volume_usdt=0.0, min_baseline_volume_usdt=0.0, exhaustion_gain_pct=0.0,
)


def _detector(**kw) -> SetupDetector:
    return SetupDetector(StrategyConfig(**STRATEGY_PARAMS), timeframe="3m", **kw)


def _quiet_bar(price: float = 100.0) -> dict:
    return {"open": price, "high": price * 1.001, "low": price * 0.999,
            "close": price, "volume": 1000.0}


def _wild_bar(prev_close: float) -> dict:
    return {"open": prev_close, "high": prev_close * 1.10, "low": prev_close * 0.90,
            "close": prev_close * 1.05, "volume": 8000.0}


class TestDetectorAtr:
    def test_baseline_ignores_pump_bars(self):
        """baseline меряет ДО sustain-окна: дикие памп-бары не влияют.

        Мутация: убрать срез [:-sustain] — baseline станет равен signal и тест упадёт.
        """
        candles = [_quiet_bar() for _ in range(20)]
        wild = []
        prev = 100.0
        for _ in range(4):
            bar = _wild_bar(prev)
            wild.append(bar)
            prev = bar["close"]
        window = candles + wild

        base = _detector(atr_period=14, atr_source="baseline").atr_pct(window)
        sig = _detector(atr_period=14, atr_source="signal").atr_pct(window)
        last_close = 100.0 * 1.05 ** 4  # wild-бары цепочкой: close ×1.05 каждый
        assert base == pytest.approx(0.2 / last_close * 100, rel=1e-6), "только тихие бары"
        # 10 тихих TR по 0.2 + 4 памп-бара с TR 20 (±10% от prev close)
        assert sig > base * 10, "памп-бары взрывают меру signal"

    def test_short_window_returns_none(self):
        candles = [_quiet_bar() for _ in range(10)]  # < period + 1
        assert _detector(atr_period=14).atr_pct(candles) is None

    def test_normalized_to_signal_close(self):
        """Стоп — % от цены входа (= close последнего бара), а не от цены самих баров ATR."""
        candles = [_quiet_bar(100.0) for _ in range(20)]
        last = dict(_quiet_bar(100.0), close=200.0, open=100.0,
                    high=200.0, low=100.0)
        window = candles + [last] * 4
        atr = _detector(atr_period=14, atr_source="baseline").atr_pct(window)
        # TR тихих баров 0.2 при цене 100, но нормируем на close 200 → вдвое меньше
        assert atr == pytest.approx(0.1, rel=1e-3)

    def test_check_price_trend_puts_measure_into_context(self):
        """Мера кладётся в контекст тем же вызовом, которым её читает движок.

        Мутация: убрать заполнение context["atr_pct"] — тест падает, а вместе с
        ним и связка движок↔прод (Signal.atr_pct остаётся None).
        """
        detector = _detector(atr_period=14)
        candles = [_quiet_bar() for _ in range(20)]
        price = 100.0
        for _ in range(4):
            nxt = price * 1.012
            candles.append({"open": price, "high": nxt * 1.001, "low": price * 0.999,
                            "close": nxt, "volume": 8000.0})
            price = nxt
        ctx: dict = {}
        assert detector.check_price_trend(candles, ctx) == "long"
        assert "atr_pct" in ctx and ctx["atr_pct"] > 0
        assert "window_range_pct" in ctx


# ---------------------------------------------------------------------------
# Движок бэктеста
# ---------------------------------------------------------------------------

BASE_TS = datetime(2026, 8, 1, 0, 0, 0)
BAR_MINUTES = 3


def _ts(i: int) -> str:
    return (BASE_TS + timedelta(minutes=BAR_MINUTES * i)).strftime("%Y-%m-%d %H:%M:%S")


def _engine_settings(**trading_overrides) -> Settings:
    strategy = StrategyConfig(**STRATEGY_PARAMS)
    params = dict(
        mode="real", max_positions=5, leverage=10, risk_per_trade_pct=1.0,
        risk_reward_ratio=2.0, stop_loss_pct=5.0, max_hold_hours=48.0,
        partial_close_pct=35.0, partial_close_qty_pct=30.0, cooldown_hours=1.0,
        backtest_slippage_pct=0.0, circuit_breaker_enabled=True,
    )
    params.update(trading_overrides)
    return Settings(exchanges={}, collectors=CollectorsConfig(timeframe="3m"),
                    strategy=strategy, trading=TradingConfig(**params))


def _trail_rows(symbol: str, n_baseline: int = 20, n_sustain: int = 4):
    """Плоский baseline (TR 0.2%) → памп-объём с ростом → путь цены выше TP.

    ATR(14, baseline) на сигнале ≈ 0.2% / close ≈ 0.19%: множитель 15 даёт
    стоп ≈ 2.86% — внутри границ (2%, 5%), т.е. без клампов.
    """
    rows = []
    price = 100.0
    for i in range(n_baseline):
        rows.append(("bybit", symbol, _ts(i), price, price * 1.001,
                     price * 0.999, price, 1_000.0))
    for j in range(n_sustain):
        i = n_baseline + j
        nxt = price * 1.012
        rows.append(("bybit", symbol, _ts(i), price, nxt * 1.001,
                     price * 0.999, nxt, 8_000.0 + j * 500))
        price = nxt
    e = price
    path = [  # (high, low, close) относительно входа
        (1.04, 0.999, 1.035),
        (1.07, 1.03, 1.065),
        (1.12, 1.06, 1.115),
        (1.18, 1.15, 1.175),
        (1.181, 1.10, 1.105),
    ]
    k = n_baseline + n_sustain
    for j, (hi, lo, cl) in enumerate(path):
        rows.append(("bybit", symbol, _ts(k + j), e, e * hi, e * lo, e * cl, 1_200.0))
    for j in range(len(path), 40):
        rows.append(("bybit", symbol, _ts(k + j), e * 1.1, e * 1.101,
                     e * 1.099, e * 1.1, 1_200.0))
    return rows, e


@pytest.fixture
def trail_db(tmp_path):
    symbol = "ATR/USDT:USDT"
    candles, entry = _trail_rows(symbol)
    oi = [("bybit", symbol, _ts(i), 1_000_000.0 * (1 + 0.01 * i)) for i in range(80)]
    path = tmp_path / "atr.db"
    db = sqlite3.connect(str(path))
    db.executescript(
        """
        CREATE TABLE candles (
            id INTEGER PRIMARY KEY, exchange TEXT, symbol TEXT, timestamp TEXT,
            open REAL, high REAL, low REAL, close REAL, volume REAL);
        CREATE TABLE open_interest (
            id INTEGER PRIMARY KEY, exchange TEXT, symbol TEXT, timestamp TEXT, value REAL);
        CREATE TABLE market_context_snapshots (
            id INTEGER PRIMARY KEY, timestamp TEXT, regime TEXT, supertrend_color TEXT);
        """
    )
    db.executemany(
        "INSERT INTO candles (exchange,symbol,timestamp,open,high,low,close,volume)"
        " VALUES (?,?,?,?,?,?,?,?)", candles)
    db.executemany(
        "INSERT INTO open_interest (exchange,symbol,timestamp,value) VALUES (?,?,?,?)", oi)
    db.commit()
    db.close()
    return str(path), entry


def test_engine_atr_stop_tightens_and_resizes(trail_db):
    """ATR-стоп сужает стоп и соразмерно УВЕЛИЧИВАЕТ объём: риск на входе тот же.

    Проверка через тождество risk = entry × qty × stop_pct/100 — размер позиции
    всегда финансирует один и тот же бюджет риска (шаг лота выключен).
    Мутация: посчитать qty от фиксированного stop_loss_pct — тождество падает.
    """
    path, _ = trail_db
    data = load_data(path)

    # партиал выключен: quantity в trades_list уже ПОСЛЕ частичного закрытия,
    # и тождество риска требуется на исходном объёме
    base = simulate(_engine_settings(partial_close_pct=0), data,
                    has_oi=True)["trades_list"]
    assert base[0]["stop_pct"] == pytest.approx(5.0)
    t0 = base[0]
    assert t0["risk"] == pytest.approx(
        t0["entry_price"] * t0["quantity"] * 0.05, rel=1e-9)

    s = _engine_settings(partial_close_pct=0, stop_loss_atr_mult=15.0)
    atr = simulate(s, data, has_oi=True)["trades_list"]
    t = atr[0]
    # ATR(14, baseline) = 0.2 абс. / close 104.887 ≈ 0.1907% × 15 ≈ 2.86%
    assert t["stop_pct"] == pytest.approx(15 * 0.2 / (100 * 1.012 ** 4) * 100, rel=0.03)
    assert 2.0 < t["stop_pct"] < 3.0
    assert t["risk"] == pytest.approx(
        t["entry_price"] * t["quantity"] * t["stop_pct"] / 100, rel=1e-9)


def test_engine_scaled_trailing_exit(trail_db):
    """Масштабированный трейлинг: отступ = ровно стоп-дистанция, выход ближе.

    При боевых пропорциях (8/5 на стопе 4%) отступ = 1.25× стопа — после
    масштабирования всегда 1.25 × stop. Пик пути 1.18, значит выход =
    peak − entry × stop_pct/100. Мутация: не передать pos.stop_pct в
    trailing_distance — выход уедет на entry × (1.18 − 0.05).
    """
    path, _ = trail_db
    data = load_data(path)
    common = dict(stop_loss_atr_mult=15.0, trailing_activation_pct=8.0,
                  trailing_distance_pct=5.0)

    fixed = simulate(_engine_settings(**common), data, has_oi=True)["trades_list"][0]
    assert fixed["exit_reason"] == "trail"
    assert fixed["exit_price"] == pytest.approx(
        fixed["entry_price"] * (1.18 - 0.05), rel=1e-9), "абсолютный отступ 5%"

    scaled = simulate(
        _engine_settings(**common, trailing_scale_with_stop=True),
        data, has_oi=True,
    )["trades_list"][0]
    assert scaled["exit_reason"] == "trail"
    assert scaled["exit_price"] == pytest.approx(
        scaled["entry_price"] * (1.18 - scaled["stop_pct"] / 100), rel=1e-9), \
        "отступ = стоп-дистанция после масштабирования"


def test_engine_atr_off_is_bitwise_baseline(trail_db):
    """mult=0 на ветке с механизмом = прогоны до правки: parity по правилу 3."""
    path, _ = trail_db
    data = load_data(path)
    legacy = simulate(_engine_settings(), data, has_oi=True)
    off = simulate(_engine_settings(stop_loss_atr_mult=0.0, stop_loss_atr_period=7,
                                    stop_loss_atr_source="signal",
                                    trailing_scale_with_stop=True),
                   data, has_oi=True)
    assert len(legacy["trades_list"]) == len(off["trades_list"])
    for a, b in zip(legacy["trades_list"], off["trades_list"]):
        assert a["pnl"] == b["pnl"] and a["exit_reason"] == b["exit_reason"]
        assert a["sl_price"] == b["sl_price"] and a["exit_price"] == b["exit_price"]


# ---------------------------------------------------------------------------
# PositionManager (прод)
# ---------------------------------------------------------------------------


def _ready_pm(connector, **overrides):
    from tests.test_position_manager import _config, _signal
    from src.executor.position_manager import PositionManager
    params = {"risk_per_trade_pct": 1.0, "stop_loss_pct": 4.0,
              "risk_reward_ratio": 2.0, "partial_close_pct": 35.0,
              "partial_close_qty_pct": 20.0,
              "trailing_activation_pct": 8.0, "trailing_distance_pct": 5.0,
              **overrides}
    pm = PositionManager(config=_config(**params), trading_connector=connector)
    pm._count_open = AsyncMock(return_value=0)  # type: ignore[method-assign]
    pm._has_position = AsyncMock(return_value=False)  # type: ignore[method-assign]
    pm._in_cooldown = AsyncMock(return_value=False)  # type: ignore[method-assign]
    pm._send_message = AsyncMock()
    return pm


def _session() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()  # синхронный метод SQLAlchemy
    return session


def _ready_connector():
    from tests.test_position_manager import _fake_connector
    return _fake_connector(
        fetch_ticker=AsyncMock(return_value={"last": 1.0}),
        fetch_balance=AsyncMock(return_value={"total": 1000.0}),
        set_leverage=AsyncMock(),
        ensure_one_way_mode=AsyncMock(),
        create_market_order=AsyncMock(return_value={"fill_price": 1.0}),
        fetch_positions=AsyncMock(return_value=[{"entry_price": 1.0}]),
        set_tpsl=AsyncMock(),
        place_reduce_only_limit=AsyncMock(return_value={"id": "1"}),
        set_trailing_stop=AsyncMock(),
        min_order_amount=AsyncMock(return_value=0.001),
    )


class TestPositionManagerAtrStop:
    async def test_atr_stop_flows_to_sl_tp_partial_and_trade(self):
        """ATR 0.5% × 6 = стоп 3%: SL −3%, партиал от TP +6% → триггер +2.1%,
        trade.stop_pct записан. Мутация: не прокинуть stop_pct в _place_market_entry
        — sl_price остаётся 0.96 и тест падает."""
        from tests.test_position_manager import _signal
        connector = _ready_connector()
        pm = _ready_pm(connector, stop_loss_atr_mult=6.0)
        signal = _signal()
        signal.atr_pct = 0.5
        with patch("asyncio.sleep", AsyncMock()):
            trade, status, _ = await pm.open_position(_session(), signal)
        assert status == "opened" and trade is not None
        assert trade.stop_pct == pytest.approx(3.0)
        kw = connector.set_tpsl.await_args.kwargs
        assert kw["sl_price"] == pytest.approx(0.97), "стоп 3%, не боевые 4%"
        assert kw["tp_price"] is None, "трейлинг включён — фиксированного TP нет"
        # риск $10 (1000 × 1%) / (1.0 × 0.03) = 333.3 контракта
        assert trade.quantity == pytest.approx(10.0 / 0.03, rel=1e-6)
        # триггер партиала: 35% пути до расчётного TP (+6%) = +2.1%
        assert connector.place_reduce_only_limit.await_args.kwargs["price"] == \
            pytest.approx(1.021)
        # трейлинг НЕ масштабирован: абсолютные 8/5
        tkw = connector.set_trailing_stop.await_args.kwargs
        assert tkw["activation_price"] == pytest.approx(1.08)
        assert tkw["distance"] == pytest.approx(0.05)

    async def test_scaled_trailing_uses_trade_stop(self):
        """trailing_scale_with_stop: активация и отступ сжаты в 3/4 (стоп 3% к 4%)."""
        from tests.test_position_manager import _signal
        connector = _ready_connector()
        pm = _ready_pm(connector, stop_loss_atr_mult=6.0,
                       trailing_scale_with_stop=True)
        signal = _signal()
        signal.atr_pct = 0.5
        with patch("asyncio.sleep", AsyncMock()):
            trade, status, _ = await pm.open_position(_session(), signal)
        assert status == "opened"
        tkw = connector.set_trailing_stop.await_args.kwargs
        assert tkw["activation_price"] == pytest.approx(1.06), "8% × 0.75"
        assert tkw["distance"] == pytest.approx(0.0375), "5% × 0.75"

    async def test_missing_measure_falls_back_to_fixed(self):
        """У сигнала нет ATR — стоп боевой, сделка не пропадает."""
        from tests.test_position_manager import _signal
        connector = _ready_connector()
        pm = _ready_pm(connector, stop_loss_atr_mult=6.0)
        with patch("asyncio.sleep", AsyncMock()):
            trade, status, _ = await pm.open_position(_session(), _signal())
        assert status == "opened"
        assert trade.stop_pct == pytest.approx(4.0)
        assert connector.set_tpsl.await_args.kwargs["sl_price"] == pytest.approx(0.96)

    def test_legacy_trade_without_stop_pct(self):
        """Сделки до 13.10.2026 (stop_pct NULL) и восстановленные sync_positions
        живут на фиксированном стопе."""
        from tests.test_position_manager import _trade
        pm = _ready_pm(_ready_connector(), stop_loss_atr_mult=6.0,
                       trailing_scale_with_stop=True)
        legacy = _trade(entry_price=100.0)
        assert pm._trade_stop_pct(legacy) == 4.0
        assert pm._tp_price(100.0, pm._trade_stop_pct(legacy)) == \
            pytest.approx(108.0)
        fresh = _trade(entry_price=100.0)
        fresh.stop_pct = 2.5
        assert pm._tp_price(100.0, pm._trade_stop_pct(fresh)) == \
            pytest.approx(105.0)
        # масштабированный пол: активация 8%×0.625 минус отступ 5%×0.625 = +1.875%
        assert pm._trailing_floor(100.0, 2.5) == pytest.approx(101.875)
