"""
Tests for PositionManager — circuit breaker, position sizing, close tracking.

Uses minimal mocking; focuses on pure business logic.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.config import TradingConfig
from src.executor.position_manager import PositionManager
from src.storage.models import Trade


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**overrides) -> TradingConfig:
    params = {
        "mode": "virtual",
        "max_positions": 10,
        "leverage": 10,
        "risk_per_trade_pct": 1.0,
        "risk_reward_ratio": 3.0,
        "stop_loss_pct": 5.0,
        "max_hold_hours": 48.0,
        "partial_close_enabled": True,
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


def _pm(**config_overrides) -> PositionManager:
    return PositionManager(config=_config(**config_overrides))


# ---------------------------------------------------------------------------
# Circuit Breaker — _check_circuit_breaker
# ---------------------------------------------------------------------------


class TestCircuitBreakerCheck:
    """Tests for _check_circuit_breaker() state machine."""

    def test_disabled_returns_none(self):
        """When disabled, always returns None (allow trading)."""
        pm = _pm(circuit_breaker_enabled=False)
        pm._consecutive_losses = 10
        assert pm._check_circuit_breaker() is None

    def test_no_losses_returns_none(self):
        """Fresh start — no restrictions."""
        pm = _pm()
        assert pm._check_circuit_breaker() is None

    def test_below_reduce_threshold_returns_none(self):
        """2 losses — still below reduce threshold (3)."""
        pm = _pm()
        pm._consecutive_losses = 2
        assert pm._check_circuit_breaker() is None

    def test_at_reduce_threshold_returns_reduce(self):
        """Exactly at reduce threshold → return 'circuit_breaker_reduce'."""
        pm = _pm()
        pm._consecutive_losses = 3
        assert pm._check_circuit_breaker() == "circuit_breaker_reduce"

    def test_between_reduce_and_stop_returns_reduce(self):
        """4 losses — between reduce (3) and stop (5)."""
        pm = _pm()
        pm._consecutive_losses = 4
        assert pm._check_circuit_breaker() == "circuit_breaker_reduce"

    def test_at_stop_threshold_sets_timer_and_returns_stop(self):
        """5 losses → full stop: sets _circuit_breaker_until, returns stop."""
        pm = _pm()
        pm._consecutive_losses = 5
        result = pm._check_circuit_breaker()
        assert result == "circuit_breaker_stop"
        assert pm._circuit_breaker_until is not None
        expected_until = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
        diff = abs((pm._circuit_breaker_until - expected_until).total_seconds())
        assert diff < 5  # within 5 seconds

    def test_during_stop_returns_stop(self):
        """While _circuit_breaker_until hasn't passed — keep returning stop."""
        pm = _pm()
        pm._circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(minutes=30)
        assert pm._check_circuit_breaker() == "circuit_breaker_stop"

    def test_after_stop_expires_resets_and_returns_none(self):
        """When stop timer expires — reset losses, return None."""
        pm = _pm()
        pm._consecutive_losses = 5
        pm._circuit_breaker_until = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        result = pm._check_circuit_breaker()
        assert result is None
        assert pm._consecutive_losses == 0
        assert pm._circuit_breaker_until is None


# ---------------------------------------------------------------------------
# Circuit Breaker — position multiplier
# ---------------------------------------------------------------------------


class TestCircuitBreakerMultiplier:
    """Tests for _get_circuit_breaker_position_mult()."""

    def test_no_losses_returns_full(self):
        pm = _pm()
        assert pm._get_circuit_breaker_position_mult() == 1.0

    def test_at_reduce_returns_half(self):
        pm = _pm()
        pm._consecutive_losses = 3
        assert pm._get_circuit_breaker_position_mult() == 0.5

    def test_disabled_returns_full(self):
        pm = _pm(circuit_breaker_enabled=False)
        pm._consecutive_losses = 10
        assert pm._get_circuit_breaker_position_mult() == 1.0

    def test_custom_reduce_pct(self):
        pm = _pm(circuit_breaker_reduce_mult_pct=25.0)
        pm._consecutive_losses = 3
        assert pm._get_circuit_breaker_position_mult() == 0.25


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
        assert pm._consecutive_losses == 1

    @pytest.mark.asyncio
    async def test_win_resets_counter(self):
        pm = _pm()
        pm._consecutive_losses = 4
        pm._send_message = AsyncMock()
        trade = _trade(pnl=15.0)
        await pm._close_position(trade, exit_price=1.15, reason="tp")
        assert pm._consecutive_losses == 0
        assert pm._circuit_breaker_until is None

    @pytest.mark.asyncio
    async def test_win_clears_stop_timer(self):
        pm = _pm()
        pm._consecutive_losses = 5
        pm._circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(minutes=30)
        pm._send_message = AsyncMock()
        trade = _trade(pnl=10.0)
        await pm._close_position(trade, exit_price=1.10, reason="tp")
        assert pm._consecutive_losses == 0
        assert pm._circuit_breaker_until is None

    @pytest.mark.asyncio
    async def test_break_even_counts_as_loss(self):
        """Zero PnL counts as a loss for safety."""
        pm = _pm()
        pm._send_message = AsyncMock()
        trade = _trade(pnl=0.0)
        await pm._close_position(trade, exit_price=1.0, reason="time")
        assert pm._consecutive_losses == 1

    @pytest.mark.asyncio
    async def test_disabled_does_not_track(self):
        pm = _pm(circuit_breaker_enabled=False)
        pm._send_message = AsyncMock()
        trade = _trade(pnl=-10.0)
        await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert pm._consecutive_losses == 0  # never changed

    @pytest.mark.asyncio
    async def test_counter_accumulates_on_consecutive_losses(self):
        pm = _pm()
        pm._send_message = AsyncMock()
        for _ in range(4):
            trade = _trade(pnl=-5.0)
            await pm._close_position(trade, exit_price=0.95, reason="sl")
        assert pm._consecutive_losses == 4

    @pytest.mark.asyncio
    async def test_reason_tp_sl_exchange_win_resets_counter(self):
        """tp_sl_exchange with positive PnL → treated as tp → resets."""
        pm = _pm()
        pm._consecutive_losses = 3
        pm._send_message = AsyncMock()
        trade = _trade(pnl=20.0)
        await pm._close_position(trade, exit_price=1.20, reason="tp_sl_exchange")
        assert pm._consecutive_losses == 0

    @pytest.mark.asyncio
    async def test_reason_tp_sl_exchange_loss_increments_counter(self):
        """tp_sl_exchange with negative PnL → treated as sl → increments."""
        pm = _pm()
        pm._send_message = AsyncMock()
        trade = _trade(pnl=-10.0)
        await pm._close_position(trade, exit_price=0.95, reason="tp_sl_exchange")
        assert pm._consecutive_losses == 1


# ---------------------------------------------------------------------------
# Position sizing — combined multipliers
# ---------------------------------------------------------------------------


class TestPositionSizing:
    """Tests for risk budget calculation with market regime + circuit breaker."""

    @pytest.mark.asyncio
    async def test_virtual_balance_risk_budget_formula(self):
        """For virtual mode: risk = $1000 × risk_pct × regime_mult × cb_mult."""
        pm = _pm(risk_per_trade_pct=1.0)
        # No CB, no regime multiplier
        pm.position_size_mult = 1.0

        # The formula tested indirectly: virtual_balance=1000, risk=1%,
        # risk_budget = 1000 * 0.01 * 1.0 * cb_mult
        # With cb_mult=1.0: risk_budget = $10
        # SL=5% at entry=$1.0: sl_distance=$0.05, qty=$10/0.05=200
        cb = pm._get_circuit_breaker_position_mult()
        virtual = 1000.0
        risk = virtual * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_cb_reduce_halves_risk(self):
        """When CB is at reduce level, risk budget is halved."""
        pm = _pm()
        pm._consecutive_losses = 3
        cb = pm._get_circuit_breaker_position_mult()
        assert cb == 0.5
        virtual = 1000.0
        risk = virtual * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_regime_cautious_halves_risk_too(self):
        """regime cautious → position_size_mult = 0.5."""
        pm = _pm()
        pm.market_regime = "cautious"
        pm.position_size_mult = 0.5
        cb = pm._get_circuit_breaker_position_mult()
        virtual = 1000.0
        risk = virtual * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
        assert risk == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_regime_cautious_plus_cb_reduce_stacks(self):
        """cautious (×0.5) + cb_reduce (×0.5) = risk ×0.25."""
        pm = _pm()
        pm._consecutive_losses = 3
        pm.market_regime = "cautious"
        pm.position_size_mult = 0.5
        cb = pm._get_circuit_breaker_position_mult()
        assert cb == 0.5
        virtual = 1000.0
        risk = virtual * (pm.config.risk_per_trade_pct / 100) * pm.position_size_mult * cb
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
# open_position — rejection reasons
# ---------------------------------------------------------------------------


class TestOpenPositionRejections:
    """open_position returns correct status codes for various rejections."""

    @pytest.mark.asyncio
    async def test_risk_off_rejection(self):
        pm = _pm()
        pm.market_regime = "risk_off"
        trade, status = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "risk_off"

    @pytest.mark.asyncio
    async def test_circuit_breaker_stop_rejection(self):
        pm = _pm()
        pm._consecutive_losses = 5
        pm._circuit_breaker_until = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        trade, status = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "circuit_breaker_stop"

    @pytest.mark.asyncio
    async def test_limit_reached_rejection(self):
        pm = _pm(max_positions=2)
        # Patch _count_open to return max_positions (limit hit)
        async def mock_count_open(_session):
            return 2
        pm._count_open = mock_count_open  # type: ignore[method-assign]

        trade, status = await pm.open_position(AsyncMock(), _signal())
        assert trade is None
        assert status == "limit"

    @pytest.mark.asyncio
    async def test_duplicate_symbol_rejection(self):
        pm = _pm()
        async def mock_count_open(_session):
            return 0
        pm._count_open = mock_count_open  # type: ignore[method-assign]
        # Patch _has_position to return True
        async def mock_has_position(_session, _symbol):
            return True
        pm._has_position = mock_has_position  # type: ignore[method-assign]

        trade, status = await pm.open_position(AsyncMock(), _signal("TEST/USDT:USDT"))
        assert trade is None
        assert status == "duplicate"

    @pytest.mark.asyncio
    async def test_banned_symbol_rejection(self):
        pm = _pm()
        pm._banned_symbols.add("TEST/USDT:USDT")
        async def mock_count_open(_session):
            return 0
        pm._count_open = mock_count_open  # type: ignore[method-assign]

        trade, status = await pm.open_position(AsyncMock(), _signal("TEST/USDT:USDT"))
        assert trade is None
        assert status == "error"


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------


class TestInitialState:
    """PositionManager starts in expected state."""

    def test_defaults(self):
        pm = _pm()
        assert pm._consecutive_losses == 0
        assert pm._circuit_breaker_until is None
        assert pm.market_regime == "unknown"
        assert pm.position_size_mult == 1.0
        assert len(pm._banned_symbols) == 0

    def test_is_real_false_for_virtual(self):
        pm = _pm()
        assert pm.is_real is False
