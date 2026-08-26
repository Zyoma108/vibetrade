"""
Tests for PositionManager — circuit breaker, position sizing, close tracking.

Uses minimal mocking; focuses on pure business logic.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.base import Signal
from src.config import TradingConfig
from src.executor import guards as guards_module
from src.executor.position_manager import PositionManager
from src.storage.models import Base, Trade


@pytest.fixture(autouse=True)
async def _isolated_bot_state_db(monkeypatch):
    """TradingGuards.save() (Circuit Breaker persistence, см. db-audit-august-2026)
    открывает свою собственную сессию через src.storage.database.async_session
    — без этой фикстуры тесты тихо стучались бы в реальный data/trading_bot.db
    (таблицы bot_state там нет, запись просто проглатывается и логируется как
    ошибка, но сам факт обращения к боевой БД из тестов — не то, что нужно)."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    test_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(guards_module, "async_session", test_session)
    yield
    await engine.dispose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**overrides) -> TradingConfig:
    params = {
        "mode": "real",
        "max_positions": 10,
        "leverage": 10,
        "risk_per_trade_pct": 1.0,
        "risk_reward_ratio": 3.0,
        "stop_loss_pct": 5.0,
        "max_hold_hours": 48.0,
        "partial_close_pct": 40.0,
        "cooldown_hours": 1.0,
        "circuit_breaker_enabled": True,
        "circuit_breaker_loss_streak_reduce": 3,
        "circuit_breaker_reduce_mult_pct": 50.0,
        "circuit_breaker_loss_streak_stop": 5,
        "circuit_breaker_stop_minutes": 60,
    }
    params.update(overrides)
    return TradingConfig(**params)


def _signal(symbol: str = "TEST/USDT:USDT", direction: str = "long") -> Signal:
    return Signal(
        symbol=symbol,
        setup_type="volume_surge",
        direction=direction,
        confidence=80,
        message="Test signal",
    )


def _trade(pnl: float = 0.0, status: str = "open", direction: str = "long",
           entry_price: float = 1.0, quantity: float = 1.0) -> Trade:
    return Trade(
        symbol="TEST/USDT:USDT",
        direction=direction,
        entry_price=entry_price,
        quantity=quantity,
        entry_time=datetime.now(tz=timezone.utc),
        status=status,
        pnl=pnl,
    )


def _fake_connector(**attrs) -> MagicMock:
    """Заглушка коннектора. `trading_connector` обязателен с 26.08.2026 — раньше он
    был Optional только ради тестов, и платил за это боевой код 26 подавлениями
    type: ignore."""
    connector = MagicMock()
    connector.exchange_id = "bybit"
    connector.has_credentials = attrs.pop("has_credentials", True)
    for name, value in attrs.items():
        setattr(connector, name, value)
    return connector


def _pm(connector: MagicMock | None = None, **config_overrides) -> PositionManager:
    return PositionManager(
        config=_config(**config_overrides),
        trading_connector=connector or _fake_connector(),
    )


# ---------------------------------------------------------------------------
# Circuit Breaker — _check_circuit_breaker
# ---------------------------------------------------------------------------


class TestCircuitBreakerCheck:
    """Tests for _check_circuit_breaker() state machine."""

    def test_disabled_returns_none(self):
        """When disabled, always returns None (allow trading)."""
        pm = _pm(circuit_breaker_enabled=False)
        pm.guards.consecutive_losses = 10
        assert pm.guards.check_circuit_breaker() is None

    def test_no_losses_returns_none(self):
        """Fresh start — no restrictions."""
        pm = _pm()
        assert pm.guards.check_circuit_breaker() is None

    def test_below_reduce_threshold_returns_none(self):
        """2 losses — still below reduce threshold (3)."""
        pm = _pm()
        pm.guards.consecutive_losses = 2
        assert pm.guards.check_circuit_breaker() is None

    def test_at_reduce_threshold_returns_reduce(self):
        """Exactly at reduce threshold → return 'circuit_breaker_reduce'."""
        pm = _pm()
        pm.guards.consecutive_losses = 3
        assert pm.guards.check_circuit_breaker() == "circuit_breaker_reduce"

    def test_between_reduce_and_stop_returns_reduce(self):
        """4 losses — between reduce (3) and stop (5)."""
        pm = _pm()
        pm.guards.consecutive_losses = 4
        assert pm.guards.check_circuit_breaker() == "circuit_breaker_reduce"

    def test_at_stop_threshold_sets_timer_and_returns_stop(self):
        """5 losses → full stop: sets _circuit_breaker_until, returns stop."""
        pm = _pm()
        pm.guards.consecutive_losses = 5
        result = pm.guards.check_circuit_breaker()
        assert result == "circuit_breaker_stop"
        assert pm.guards.circuit_breaker_until is not None
        expected_until = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
        diff = abs((pm.guards.circuit_breaker_until - expected_until).total_seconds())
        assert diff < 5  # within 5 seconds

    def test_during_stop_returns_stop(self):
        """While _circuit_breaker_until hasn't passed — keep returning stop."""
        pm = _pm()
        pm.guards.circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(minutes=30)
        assert pm.guards.check_circuit_breaker() == "circuit_breaker_stop"

    def test_after_stop_expires_keeps_streak_and_returns_reduce(self):
        """When stop timer expires — do NOT reset the loss streak (only a win does that);
        resume trading in reduced-size mode instead of silently forgetting the streak."""
        pm = _pm()
        pm.guards.consecutive_losses = 5
        pm.guards.circuit_breaker_stop_consumed_at = 5
        pm.guards.circuit_breaker_until = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        result = pm.guards.check_circuit_breaker()
        assert result == "circuit_breaker_reduce"
        assert pm.guards.consecutive_losses == 5
        assert pm.guards.circuit_breaker_until is None

    def test_new_loss_after_expiry_retriggers_full_stop(self):
        """A loss streak that grows PAST the already-served stop threshold re-triggers
        a fresh full stop, even though the timer previously expired."""
        pm = _pm()
        pm.guards.consecutive_losses = 5
        pm.guards.circuit_breaker_stop_consumed_at = 5
        pm.guards.circuit_breaker_until = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        assert pm.guards.check_circuit_breaker() == "circuit_breaker_reduce"

        pm.guards.consecutive_losses = 6  # one more loss arrives while in reduce-mode
        result = pm.guards.check_circuit_breaker()
        assert result == "circuit_breaker_stop"
        assert pm.guards.circuit_breaker_until is not None
        assert pm.guards.circuit_breaker_stop_consumed_at == 6


# ---------------------------------------------------------------------------
# Circuit Breaker — position multiplier
# ---------------------------------------------------------------------------


class TestCircuitBreakerMultiplier:
    """Tests for _get_circuit_breaker_position_mult()."""

    def test_no_losses_returns_full(self):
        pm = _pm()
        assert pm.guards.position_size_mult() == 1.0

    def test_at_reduce_returns_half(self):
        pm = _pm()
        pm.guards.consecutive_losses = 3
        assert pm.guards.position_size_mult() == 0.5

    def test_disabled_returns_full(self):
        pm = _pm(circuit_breaker_enabled=False)
        pm.guards.consecutive_losses = 10
        assert pm.guards.position_size_mult() == 1.0

    def test_custom_reduce_pct(self):
        pm = _pm(circuit_breaker_reduce_mult_pct=25.0)
        pm.guards.consecutive_losses = 3
        assert pm.guards.position_size_mult() == 0.25


# ---------------------------------------------------------------------------
# Close position — counter tracking
# ---------------------------------------------------------------------------


class TestClosePositionCounter:
    """Tests for _close_position counter updates."""

    @pytest.mark.asyncio
    async def test_loss_increments_counter(self):
        pm = _pm()
        pm._send_message = AsyncMock()
        trade = _trade(pnl=-10.0)
        await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert pm.guards.consecutive_losses == 1

    @pytest.mark.asyncio
    async def test_win_resets_counter(self):
        pm = _pm()
        pm.guards.consecutive_losses = 4
        pm._send_message = AsyncMock()
        trade = _trade(pnl=15.0)
        await pm._close_position(trade, exit_price=1.15, reason="tp")
        assert pm.guards.consecutive_losses == 0
        assert pm.guards.circuit_breaker_until is None

    @pytest.mark.asyncio
    async def test_win_clears_stop_timer(self):
        pm = _pm()
        pm.guards.consecutive_losses = 5
        pm.guards.circuit_breaker_stop_consumed_at = 5
        pm.guards.circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(minutes=30)
        pm._send_message = AsyncMock()
        trade = _trade(pnl=10.0)
        await pm._close_position(trade, exit_price=1.10, reason="tp")
        assert pm.guards.consecutive_losses == 0
        assert pm.guards.circuit_breaker_until is None
        assert pm.guards.circuit_breaker_stop_consumed_at == 0

    @pytest.mark.asyncio
    async def test_break_even_counts_as_loss(self):
        """Zero PnL counts as a loss for safety."""
        pm = _pm()
        pm._send_message = AsyncMock()
        trade = _trade(pnl=0.0)
        await pm._close_position(trade, exit_price=1.0, reason="time")
        assert pm.guards.consecutive_losses == 1

    @pytest.mark.asyncio
    async def test_disabled_does_not_track(self):
        pm = _pm(circuit_breaker_enabled=False)
        pm._send_message = AsyncMock()
        trade = _trade(pnl=-10.0)
        await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert pm.guards.consecutive_losses == 0  # never changed

    @pytest.mark.asyncio
    async def test_counter_accumulates_on_consecutive_losses(self):
        pm = _pm()
        pm._send_message = AsyncMock()
        for _ in range(4):
            trade = _trade(pnl=-5.0)
            await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert pm.guards.consecutive_losses == 4

    @pytest.mark.asyncio
    async def test_reason_tp_sl_exchange_win_resets_counter(self):
        """tp_sl_exchange with positive PnL → treated as tp → resets."""
        pm = _pm()
        pm.guards.consecutive_losses = 3
        pm._send_message = AsyncMock()
        trade = _trade(pnl=20.0)
        await pm._close_position(trade, exit_price=1.20, reason="tp_sl_exchange")
        assert pm.guards.consecutive_losses == 0

    @pytest.mark.asyncio
    async def test_reason_tp_sl_exchange_loss_increments_counter(self):
        """tp_sl_exchange with negative PnL → treated as sl → increments."""
        pm = _pm()
        pm._send_message = AsyncMock()
        trade = _trade(pnl=-10.0)
        await pm._close_position(trade, exit_price=0.95, reason="tp_sl_exchange")
        assert pm.guards.consecutive_losses == 1


# ---------------------------------------------------------------------------
# Position sizing — combined multipliers
# ---------------------------------------------------------------------------


class TestPositionSizing:
    """Tests for risk budget calculation with market regime + circuit breaker."""

    @pytest.mark.asyncio
    async def test_risk_budget_formula(self):
        """Risk = balance × risk_pct × regime_mult × cb_mult."""
        pm = _pm(risk_per_trade_pct=1.0)
        pm.position_size_mult = 1.0
        cb = pm.guards.position_size_mult()
        balance = 1000.0
        risk = balance * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_cb_reduce_halves_risk(self):
        """When CB is at reduce level, risk budget is halved."""
        pm = _pm()
        pm.guards.consecutive_losses = 3
        cb = pm.guards.position_size_mult()
        assert cb == 0.5
        balance = 1000.0
        risk = balance * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_regime_cautious_halves_risk_too(self):
        """regime cautious → position_size_mult = 0.5."""
        pm = _pm()
        pm.market_regime = "cautious"
        pm.position_size_mult = 0.5
        cb = pm.guards.position_size_mult()
        balance = 1000.0
        risk = balance * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_regime_cautious_plus_cb_reduce_stacks(self):
        """cautious (×0.5) + cb_reduce (×0.5) = risk ×0.25."""
        pm = _pm()
        pm.guards.consecutive_losses = 3
        pm.market_regime = "cautious"
        pm.position_size_mult = 0.5
        cb = pm.guards.position_size_mult()
        assert cb == 0.5
        balance = 1000.0
        risk = balance * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(2.5)


# ---------------------------------------------------------------------------
# TP/SL price calculations
# ---------------------------------------------------------------------------


class TestPriceCalculations:
    """TP and SL price formulas."""

    def test_tp_price_long(self):
        pm = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0)
        tp = pm._tp_price(entry=1.0)
        # SL distance = 1.0 × 5% = 0.05
        # TP = 1.0 + 0.05 × 3.0 = 1.15
        assert tp == pytest.approx(1.15)

    def test_sl_price_long(self):
        pm = _pm(stop_loss_pct=5.0)
        sl = pm._sl_price(entry=1.0)
        # SL = 1.0 × (1 - 5%) = 0.95
        assert sl == pytest.approx(0.95)

    def test_tp_price_custom_rr(self):
        pm = _pm(stop_loss_pct=10.0, risk_reward_ratio=2.0)
        tp = pm._tp_price(entry=100.0)
        # SL distance = 100 × 10% = 10
        # TP = 100 + 10 × 2.0 = 120
        assert tp == pytest.approx(120.0)

    def test_sl_price_small_entry(self):
        pm = _pm(stop_loss_pct=5.0)
        sl = pm._sl_price(entry=0.001)
        assert sl == pytest.approx(0.00095)


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------


class TestFeeCalculation:
    """_fee(): taker/maker комиссия от notional."""

    def test_taker_fee(self):
        pm = _pm(taker_fee_pct=0.055, maker_fee_pct=0.02)
        fee = pm._fee(1000.0, taker=True)
        assert fee == pytest.approx(0.55)

    def test_maker_fee_cheaper_than_taker(self):
        pm = _pm(taker_fee_pct=0.055, maker_fee_pct=0.02)
        assert pm._fee(1000.0, taker=False) < pm._fee(1000.0, taker=True)

    def test_zero_fee_config(self):
        pm = _pm(taker_fee_pct=0.0, maker_fee_pct=0.0)
        assert pm._fee(1000.0, taker=True) == 0.0

    @pytest.mark.asyncio
    async def test_close_position_deducts_exit_fee_from_pnl(self):
        """_close_position добавляет exit-fee к trade.fee и вычитает из pnl.

        tp_as_limit_order=False — тестируем базовый taker-путь; maker-путь
        для TP покрыт отдельно в TestTakeProfitAsLimitOrder ниже."""
        pm = _pm(taker_fee_pct=0.055, tp_as_limit_order=False)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.fee = 0.055  # комиссия входа, уже начислена при open_position

        # remainder = (1.05 - 1.0) * 100 = 5.0
        # exit_fee = 1.05 * 100 * 0.055% = 0.05775
        # trade.fee итого = 0.055 + 0.05775 = 0.11275
        # pnl = 5.0 - 0.11275 = 4.88725
        await pm._close_position(trade, exit_price=1.05, reason="tp")
        assert trade.fee == pytest.approx(0.11275)
        assert trade.pnl == pytest.approx(4.88725)

    @pytest.mark.asyncio
    async def test_close_position_handles_none_fee(self):
        """Позиции, восстановленные при sync (без известной fee), не должны падать."""
        pm = _pm(taker_fee_pct=0.055, tp_as_limit_order=False)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.fee = None  # например, позиция восстановлена после рестарта бота

        await pm._close_position(trade, exit_price=1.05, reason="tp")
        assert trade.fee == pytest.approx(0.05775)  # только exit-fee


class TestTakeProfitAsLimitOrder:
    """_close_position: TP закрывается лимитным ордером (maker) при
    tp_as_limit_order=True — комиссия/PnL должны считаться по maker-ставке,
    а не по дефолтной taker (см. AGENTS.md, tp_as_limit_order)."""

    @pytest.mark.asyncio
    async def test_tp_uses_maker_fee_when_enabled(self):
        pm = _pm(taker_fee_pct=0.055, maker_fee_pct=0.02, tp_as_limit_order=True)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.fee = 0.055  # комиссия входа (всегда taker — market)

        # exit_fee = 1.05 * 100 * 0.02% (maker) = 0.021
        await pm._close_position(trade, exit_price=1.05, reason="tp")
        assert trade.fee == pytest.approx(0.076)
        assert trade.pnl == pytest.approx(4.924)

    @pytest.mark.asyncio
    async def test_sl_stays_taker_even_when_tp_as_limit_enabled(self):
        """SL всегда market/taker независимо от tp_as_limit_order — надёжность
        выхода важнее экономии на комиссии (см. AGENTS.md)."""
        pm = _pm(taker_fee_pct=0.055, maker_fee_pct=0.02, tp_as_limit_order=True)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.fee = 0.055

        await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert trade.fee == pytest.approx(0.055 + 0.95 * 100 * 0.00055)

    @pytest.mark.asyncio
    async def test_tp_sl_exchange_inferred_tp_uses_maker_fee(self):
        """reason='tp_sl_exchange' с pnl>0 определяется как TP — тоже должен
        получить maker-комиссию, а не застрять на предварительной taker-оценке."""
        pm = _pm(taker_fee_pct=0.055, maker_fee_pct=0.02, tp_as_limit_order=True)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.fee = 0.055

        await pm._close_position(trade, exit_price=1.05, reason="tp_sl_exchange")
        assert trade.fee == pytest.approx(0.076)


# ---------------------------------------------------------------------------
# open_position — rejection reasons
# ---------------------------------------------------------------------------


class TestOpenPositionRejections:
    """open_position returns correct status codes for various rejections."""

    @pytest.mark.asyncio
    async def test_market_block_rejection(self):
        pm = _pm()
        pm.block_entries = True
        trade, status, detail = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "market_block"

    @pytest.mark.asyncio
    async def test_circuit_breaker_stop_rejection(self):
        pm = _pm()
        pm.guards.consecutive_losses = 5
        pm.guards.circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        trade, status, detail = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "circuit_breaker_stop"

    @pytest.mark.asyncio
    async def test_limit_reached_rejection(self):
        pm = _pm(max_positions=2)
        async def mock_count_open(_session):
            return 2
        pm._count_open = mock_count_open  # type: ignore[method-assign]

        trade, status, detail = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "limit"
        assert detail is not None

    @pytest.mark.asyncio
    async def test_duplicate_symbol_rejection(self):
        pm = _pm()
        async def mock_count_open(_session):
            return 0
        pm._count_open = mock_count_open  # type: ignore[method-assign]
        async def mock_has_position(_session, _symbol):
            return True
        pm._has_position = mock_has_position  # type: ignore[method-assign]

        trade, status, detail = await pm.open_position(AsyncMock(), _signal("TEST/USDT:USDT"))
        assert trade is None
        assert status == "duplicate"

    @pytest.mark.asyncio
    async def test_banned_symbol_rejection(self):
        pm = _pm()
        pm.guards.banned_symbols.add("TEST/USDT:USDT")
        async def mock_count_open(_session):
            return 0
        pm._count_open = mock_count_open  # type: ignore[method-assign]

        trade, status, detail = await pm.open_position(AsyncMock(), _signal("TEST/USDT:USDT"))
        assert trade is None
        assert status == "error"
        assert detail == "banned_symbol"


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Error cascade protection
# ---------------------------------------------------------------------------


class TestErrorCascade:
    """_track_error / _reset_errors — защита от каскада ошибок."""

    def test_first_error_no_cooldown(self):
        """Одна ошибка — ещё нет кулдауна."""
        pm = _pm()
        pm.guards.track_error("TEST/USDT:USDT")
        assert pm.guards.error_counts["TEST/USDT:USDT"] == 1
        assert "TEST/USDT:USDT" not in pm.guards.error_cooldown_until

    def test_third_error_triggers_cooldown(self):
        """Три ошибки подряд → кулдаун 4 часа."""
        pm = _pm()
        for _ in range(3):
            pm.guards.track_error("TEST/USDT:USDT")
        assert pm.guards.error_counts["TEST/USDT:USDT"] == 3
        assert "TEST/USDT:USDT" in pm.guards.error_cooldown_until
        until = pm.guards.error_cooldown_until["TEST/USDT:USDT"]
        expected = datetime.now(tz=timezone.utc) + timedelta(hours=4)
        assert abs((until - expected).total_seconds()) < 5

    def test_fourth_error_stays_in_cooldown(self):
        """Четвёртая ошибка — счётчик растёт, кулдаун остаётся."""
        pm = _pm()
        for _ in range(4):
            pm.guards.track_error("TEST/USDT:USDT")
        assert pm.guards.error_counts["TEST/USDT:USDT"] == 4
        assert "TEST/USDT:USDT" in pm.guards.error_cooldown_until

    def test_reset_clears_counter_and_cooldown(self):
        """Успешная сделка сбрасывает счётчик ошибок."""
        pm = _pm()
        for _ in range(5):
            pm.guards.track_error("TEST/USDT:USDT")
        pm.guards.reset_errors("TEST/USDT:USDT")
        assert "TEST/USDT:USDT" not in pm.guards.error_counts
        assert "TEST/USDT:USDT" not in pm.guards.error_cooldown_until

    def test_different_symbols_independent(self):
        """Ошибки по разным символам считаются независимо."""
        pm = _pm()
        pm.guards.track_error("A/USDT:USDT")
        pm.guards.track_error("A/USDT:USDT")
        pm.guards.track_error("B/USDT:USDT")
        assert pm.guards.error_counts["A/USDT:USDT"] == 2
        assert pm.guards.error_counts["B/USDT:USDT"] == 1
        assert "A/USDT:USDT" not in pm.guards.error_cooldown_until  # < 3

    @pytest.mark.asyncio
    async def test_cooldown_blocks_signal(self):
        """Сигнал в кулдауне возвращает error с detail."""
        pm = _pm()
        # Имитируем 3 ошибки и кулдаун
        pm.guards.error_counts["TEST/USDT:USDT"] = 3
        pm.guards.error_cooldown_until["TEST/USDT:USDT"] = (
            datetime.now(tz=timezone.utc) + timedelta(hours=2)
        )

        async def mock_count_open(_session):
            return 0
        pm._count_open = mock_count_open  # type: ignore[method-assign]

        trade, status, detail = await pm.open_position(
            AsyncMock(), _signal("TEST/USDT:USDT")
        )
        assert trade is None
        assert status == "error"
        assert detail and "error_cooldown" in detail


# ---------------------------------------------------------------------------
# Exchange error alerting — _alert_exchange_error / _clear_exchange_error
# (см. db-audit-august-19-2026: истёкший API-ключ ~40ч не давал алертов)
# ---------------------------------------------------------------------------


class TestExchangeErrorAlert:
    def _pm_with_notifier(self) -> tuple[PositionManager, AsyncMock]:
        send_message = AsyncMock()
        pm = PositionManager(
            config=_config(), trading_connector=_fake_connector(), send_message=send_message,
        )
        return pm, send_message

    @pytest.mark.asyncio
    async def test_first_error_alerts_immediately(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error("fetch_balance", Exception("boom"))
        await pm.flush_notifications()
        send_message.assert_awaited_once()
        assert "boom" in send_message.await_args.args[0]
        assert pm._exchange_error_since is not None

    @pytest.mark.asyncio
    async def test_repeated_error_within_cooldown_does_not_realert(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error("fetch_balance", Exception("boom"))
        await pm._alert_exchange_error("fetch_balance", Exception("boom again"))
        await pm.flush_notifications()
        send_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_after_cooldown_realerts_with_downtime(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error("fetch_balance", Exception("boom"))
        pm._exchange_error_since -= timedelta(minutes=31)  # type: ignore[operator]
        pm._exchange_error_last_alert_at -= timedelta(minutes=31)  # type: ignore[operator]
        await pm._alert_exchange_error("fetch_balance", Exception("still down"))
        await pm.flush_notifications()
        assert send_message.await_count == 2
        assert "продолжается" in send_message.await_args.args[0]

    @pytest.mark.asyncio
    async def test_auth_like_error_includes_hint(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error(
            "fetch_balance", Exception("Your api key has expired.")
        )
        await pm.flush_notifications()
        assert "ключ" in send_message.await_args.args[0]

    @pytest.mark.asyncio
    async def test_non_auth_error_has_no_hint(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error("fetch_balance", Exception("network timeout"))
        await pm.flush_notifications()
        assert "ключ" not in send_message.await_args.args[0]

    @pytest.mark.asyncio
    async def test_clear_after_error_sends_recovery_message(self):
        pm, send_message = self._pm_with_notifier()
        await pm._alert_exchange_error("fetch_balance", Exception("boom"))
        await pm._clear_exchange_error()
        await pm.flush_notifications()
        assert send_message.await_count == 2
        assert "восстановлена" in send_message.await_args.args[0]
        assert pm._exchange_error_since is None
        assert pm._exchange_error_last_alert_at is None

    @pytest.mark.asyncio
    async def test_clear_without_prior_error_is_noop(self):
        pm, send_message = self._pm_with_notifier()
        await pm._clear_exchange_error()
        await pm.flush_notifications()
        send_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_notifier_configured_does_not_raise(self):
        pm = PositionManager(
            config=_config(), trading_connector=_fake_connector(),
        )  # send_message=None
        await pm._alert_exchange_error("fetch_balance", Exception("boom"))
        await pm._clear_exchange_error()


class TestInitialState:
    """PositionManager starts in expected state."""

    def test_defaults(self):
        pm = _pm()
        assert pm.guards.consecutive_losses == 0
        assert pm.guards.circuit_breaker_until is None
        assert pm.guards.circuit_breaker_stop_consumed_at == 0
        assert pm.market_regime == "unknown"
        assert pm.position_size_mult == 1.0
        assert len(pm.guards.banned_symbols) == 0



# ---------------------------------------------------------------------------
# Partial close trigger price
# ---------------------------------------------------------------------------


class TestPartialCloseTrigger:
    """Формула цены частичной фиксации: entry + (tp - entry) × partial_close_pct%."""

    def test_default_40pct_of_way_to_tp(self):
        """При partial_close_pct=40%, trigger = entry + 40% пути до TP."""
        pm = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0, partial_close_pct=40.0)
        entry = 1.0
        tp = pm._tp_price(entry)  # 1.15
        trigger = entry + (tp - entry) * (pm.config.partial_close_pct / 100)
        # trigger = 1.0 + (1.15 - 1.0) × 0.40 = 1.0 + 0.06 = 1.06
        assert trigger == pytest.approx(1.06)

    def test_50pct_halfway_to_tp(self):
        """При partial_close_pct=50%, trigger = ровно половина пути до TP."""
        pm = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0, partial_close_pct=50.0)
        entry = 1.0
        tp = pm._tp_price(entry)  # 1.15
        trigger = entry + (tp - entry) * (pm.config.partial_close_pct / 100)
        # trigger = 1.0 + 0.15 × 0.50 = 1.075
        assert trigger == pytest.approx(1.075)

    def test_trigger_below_tp(self):
        """Триггер частичной фиксации всегда ниже TP."""
        pm = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0, partial_close_pct=90.0)
        entry = 1.0
        tp = pm._tp_price(entry)  # 1.15
        trigger = entry + (tp - entry) * (pm.config.partial_close_pct / 100)
        assert trigger < tp

    def test_trigger_above_entry(self):
        """Триггер частичной фиксации всегда выше entry (для long)."""
        pm = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0, partial_close_pct=10.0)
        entry = 1.0
        tp = pm._tp_price(entry)
        trigger = entry + (tp - entry) * (pm.config.partial_close_pct / 100)
        assert trigger > entry

    def test_different_rr_affects_trigger(self):
        """При RR=2.0 вместо 3.0, путь до TP короче → trigger ближе к entry."""
        pm_rr3 = _pm(stop_loss_pct=5.0, risk_reward_ratio=3.0, partial_close_pct=40.0)
        pm_rr2 = _pm(stop_loss_pct=5.0, risk_reward_ratio=2.0, partial_close_pct=40.0)
        entry = 1.0
        trigger_rr3 = entry + (pm_rr3._tp_price(entry) - entry) * 0.40
        trigger_rr2 = entry + (pm_rr2._tp_price(entry) - entry) * 0.40
        # RR=3: trigger = 1.06, RR=2: trigger = 1.04
        assert trigger_rr2 < trigger_rr3


# ---------------------------------------------------------------------------
# Partial close — combined PnL
# ---------------------------------------------------------------------------


class TestPartialClosePnL:
    """Суммарный PnL = PnL остатка + partial_pnl."""

    @pytest.mark.asyncio
    async def test_combined_pnl_with_partial_profit(self):
        """Частичная фиксация в плюс + остаток в плюс → сумма больше."""
        pm = _pm()
        pm._send_message = AsyncMock()

        # Симулируем частичное закрытие: было quantity=200, закрыли 100 в плюс
        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.partial_closed = True
        trade.partial_pnl = 3.0  # +3 USDT от частичной фиксации

        # Закрываем остаток с убытком -1 USDT
        # remainder_pnl = (0.99 - 1.0) * 100 = -1.0
        # exit_fee (taker) = 0.99 * 100 * 0.055% = 0.05445
        # total = -1.0 + 3.0 - 0.05445 = 1.94555
        await pm._close_position(trade, exit_price=0.99, reason="sl")
        assert trade.pnl == pytest.approx(1.94555)

    @pytest.mark.asyncio
    async def test_partial_profit_saves_losing_trade(self):
        """Частичная фиксация может сделать общий PnL положительным
        даже при убыточном остатке."""
        pm = _pm()
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=50, pnl=0.0)
        trade.partial_closed = True
        trade.partial_pnl = 5.0

        # Остаток: (0.94 - 1.0) * 50 = -3.0
        # gross total = -3.0 + 5.0 = 2.0, минус небольшая комиссия выхода → прибыль!
        await pm._close_position(trade, exit_price=0.94, reason="sl")
        assert trade.pnl > 0

    @pytest.mark.asyncio
    async def test_partial_loss_drags_down_winner(self):
        """Отрицательный partial_pnl уменьшает общий PnL.

        tp_as_limit_order=False — тест про комбинирование partial_pnl+remainder,
        не про тип TP-ордера (тот покрыт в TestTakeProfitAsLimitOrder)."""
        pm = _pm(tp_as_limit_order=False)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        trade.partial_closed = True
        trade.partial_pnl = -2.0

        # Остаток: (1.10 - 1.0) * 100 = +10.0
        # exit_fee (taker) = 1.10 * 100 * 0.055% = 0.0605
        # total = +10.0 + (-2.0) - 0.0605 = 7.9395
        await pm._close_position(trade, exit_price=1.10, reason="tp")
        assert trade.pnl == pytest.approx(7.9395)

    @pytest.mark.asyncio
    async def test_no_partial_pnl_is_zero(self):
        """Без частичного закрытия partial_pnl = 0."""
        pm = _pm(tp_as_limit_order=False)
        pm._send_message = AsyncMock()

        trade = _trade(entry_price=1.0, quantity=100, pnl=0.0)
        # partial_closed=False, partial_pnl=0.0 (default)
        # exit_fee (taker) = 1.05 * 100 * 0.055% = 0.05775
        await pm._close_position(trade, exit_price=1.05, reason="tp")
        assert trade.pnl == pytest.approx(4.94225)  # от остатка минус комиссия выхода


# ---------------------------------------------------------------------------
# MarketContext — should_block_entries
# ---------------------------------------------------------------------------


class TestMarketContextBlockEntries:
    """should_block_entries: риск-режимы и CAUTIOUS+ST=red."""

    def _make_mc(self, enabled=True, ready=True, regime="risk_on",
                 supertrend_color="green"):
        """Создать MarketContext с заданным состоянием."""
        from unittest.mock import MagicMock
        from src.analytics.market_context import MarketContext
        from src.config import MarketContextConfig

        cfg = MarketContextConfig(
            enabled=enabled,
            btc_drop_threshold_pct=1.5,
            supertrend_atr_period=10,
            supertrend_multiplier=3.0,
            trend_threshold_pct=0.3,
        )
        connector = MagicMock()
        mc = MarketContext(cfg, connector)
        mc._ready = ready
        mc._regime = regime
        mc._supertrend_color = supertrend_color
        return mc

    def test_disabled_never_blocks(self):
        """MarketContext выключен → не блокируем."""
        mc = self._make_mc(enabled=False, regime="risk_off")
        assert mc.should_block_entries() is False

    def test_not_ready_never_blocks(self):
        """Ещё нет данных → не блокируем."""
        mc = self._make_mc(ready=False, regime="risk_off")
        assert mc.should_block_entries() is False

    def test_risk_off_blocks(self):
        """RISK-OFF всегда блокирует."""
        mc = self._make_mc(regime="risk_off", supertrend_color="green")
        assert mc.should_block_entries() is True

    def test_risk_on_allows(self):
        """RISK-ON всегда разрешает."""
        mc = self._make_mc(regime="risk_on", supertrend_color="red")
        assert mc.should_block_entries() is False

    def test_cautious_st_green_allows(self):
        """CAUTIOUS + ST=green → разрешает входы."""
        mc = self._make_mc(regime="cautious", supertrend_color="green")
        assert mc.should_block_entries() is False

    def test_cautious_st_red_blocks(self):
        """CAUTIOUS + ST=red → блокирует (аудит июня 2026)."""
        mc = self._make_mc(regime="cautious", supertrend_color="red")
        assert mc.should_block_entries() is True

    def test_unknown_allows(self):
        """Неизвестный режим → не блокируем (ждём данных)."""
        mc = self._make_mc(regime="unknown", supertrend_color="red")
        assert mc.should_block_entries() is False


# ---------------------------------------------------------------------------
# Fallback частичной фиксации — _check_partial_close_fallback
# ---------------------------------------------------------------------------


class TestPartialCloseFallback:
    """Fallback срабатывает, только когда на бирже ТОЧНО нет живого лимитника.

    Регрессия 26.08.2026: сбой `fetch_open_orders` проглатывался, флаг
    `has_open_orders` оставался False, и market reduce-only уходил поверх
    работающего лимитника — закрывалось вдвое больше запланированного, молча.
    """

    @staticmethod
    def _pm_with_connector(open_orders_result, **overrides) -> tuple[PositionManager, MagicMock]:
        connector = _fake_connector(
            create_market_reduce_order=AsyncMock(return_value={}),
            fetch_positions=AsyncMock(return_value=[]),
            set_tpsl=AsyncMock(),
        )
        if isinstance(open_orders_result, Exception):
            connector.fetch_open_orders = AsyncMock(side_effect=open_orders_result)
        else:
            connector.fetch_open_orders = AsyncMock(return_value=open_orders_result)
        pm = _pm(connector, partial_close_pct=40.0, partial_close_qty_pct=30.0, **overrides)
        return pm, connector

    @staticmethod
    def _triggered_price(pm: PositionManager, pos: Trade) -> float:
        """Цена ровно на пороге частичной фиксации."""
        return pm._partial_trigger_price(pos.entry_price, pm._tp_price(pos.entry_price))

    async def test_below_trigger_does_nothing(self):
        """Ниже порога — fallback вообще не рассматривается."""
        pm, connector = self._pm_with_connector([])
        pos = _trade(entry_price=100.0, quantity=10.0)
        assert await pm._check_partial_close_fallback(pos, 100.5) is False
        connector.create_market_reduce_order.assert_not_called()

    async def test_existing_limit_order_blocks_fallback(self):
        """Лимитник уже работает — market-дубль не отправляем."""
        pm, connector = self._pm_with_connector([{"id": "1"}])
        pos = _trade(entry_price=100.0, quantity=10.0)
        handled = await pm._check_partial_close_fallback(pos, self._triggered_price(pm, pos))
        assert handled is True
        connector.create_market_reduce_order.assert_not_called()
        assert not pos.partial_closed

    async def test_order_check_failure_skips_cycle(self):
        """Сбой проверки ордеров = неизвестное состояние → пропускаем цикл.

        Это и есть исходный баг: раньше исключение означало «ордеров нет».
        """
        pm, connector = self._pm_with_connector(TimeoutError("exchange timeout"))
        pos = _trade(entry_price=100.0, quantity=10.0)
        handled = await pm._check_partial_close_fallback(pos, self._triggered_price(pm, pos))
        assert handled is True
        connector.create_market_reduce_order.assert_not_called()
        assert not pos.partial_closed
        assert pos.quantity == 10.0  # позиция не тронута

    async def test_no_orders_fires_fallback(self):
        """Ордеров действительно нет — fallback отрабатывает и двигает стоп в б/у."""
        pm, connector = self._pm_with_connector([])
        pos = _trade(entry_price=100.0, quantity=10.0)
        handled = await pm._check_partial_close_fallback(pos, self._triggered_price(pm, pos))
        assert handled is True
        connector.create_market_reduce_order.assert_awaited_once()
        assert pos.partial_closed is True
        assert pos.quantity == pytest.approx(7.0)  # закрыто 30%
        assert pos.current_sl_price == pos.entry_price  # стоп в безубыток


# ---------------------------------------------------------------------------
# Цена входа — живая с биржи (26.08.2026)
# ---------------------------------------------------------------------------


class TestEntryPrice:
    """Вход считается по ЖИВОЙ цене, а не по сохранённому тикеру.

    Сборщик обновляет тикер раз в цикл (на проде ~135 с между записями по монете),
    а market-ордер исполняется по текущей цене — вся разница оседала в
    проскальзывании, которое backtest_slippage_pct подбирал вслепую.
    """

    async def test_uses_live_ticker(self):
        connector = _fake_connector(fetch_ticker=AsyncMock(return_value={"last": 123.45}))
        pm = _pm(connector)
        price = await pm._get_entry_price(None, "TEST/USDT:USDT")
        assert price == 123.45
        connector.fetch_ticker.assert_awaited_once_with("TEST/USDT:USDT")

    async def test_falls_back_to_stored_price_on_error(self, monkeypatch):
        """Не получить цену вообще хуже, чем получить слегка устаревшую."""
        connector = _fake_connector(fetch_ticker=AsyncMock(side_effect=TimeoutError("down")))
        pm = _pm(connector)
        monkeypatch.setattr(pm, "_get_current_price", AsyncMock(return_value=99.0))
        assert await pm._get_entry_price(None, "TEST/USDT:USDT") == 99.0

    async def test_falls_back_when_ticker_has_no_price(self, monkeypatch):
        connector = _fake_connector(fetch_ticker=AsyncMock(return_value={"last": None}))
        pm = _pm(connector)
        monkeypatch.setattr(pm, "_get_current_price", AsyncMock(return_value=88.0))
        assert await pm._get_entry_price(None, "TEST/USDT:USDT") == 88.0
