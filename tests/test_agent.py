"""
Tests for the AI-mode agent subsystem: PositionManager's agent-facing methods
(tighten SL, extend hold, early close, source scoping) and AgentToolkit's data
tools / strategy briefing (used by scripts/agent_data.py, agent_briefing.py —
the CLI surface entry-agent/reeval-agent call via Bash).

Uses minimal mocking, mirrors the conventions in test_position_manager.py.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.agent.tools import AgentToolkit, build_strategy_briefing
from src.config import AgentConfig, StrategyConfig, TradingConfig
from src.executor.position_manager import PositionManager
from src.storage.models import AgentDecision, Base, MarketContextSnapshot, Trade

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _trading_config(**overrides) -> TradingConfig:
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
    }
    params.update(overrides)
    return TradingConfig(**params)


def _agent_config(**overrides) -> AgentConfig:
    params = {
        "enabled": True,
        "dry_run": False,
        "max_hold_extension_hours": 12.0,
        "max_hold_extension_total_hours": 24.0,
    }
    params.update(overrides)
    return AgentConfig(**params)


def _trade(**overrides) -> Trade:
    params = dict(
        symbol="TEST/USDT:USDT",
        direction="long",
        entry_price=100.0,
        quantity=1.0,
        entry_time=datetime.now(tz=timezone.utc),
        status="open",
        source="agent",
    )
    params.update(overrides)
    return Trade(**params)


def _agent_pm(**agent_overrides) -> PositionManager:
    pm = PositionManager(
        config=_trading_config(),
        source="agent",
        agent_config=_agent_config(**agent_overrides),
    )
    pm._connector = MagicMock()
    pm._connector.set_tpsl = AsyncMock()
    pm._connector.cancel_all_orders = AsyncMock()
    pm._connector.close_position = AsyncMock()
    return pm


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as sess:
        yield sess


# ---------------------------------------------------------------------------
# apply_agent_tighten_sl — never loosen, enforced in code
# ---------------------------------------------------------------------------


class TestApplyAgentTightenSL:
    @pytest.mark.asyncio
    async def test_rejects_loosening(self):
        pm = _agent_pm()
        pos = _trade(current_sl_price=95.0)
        ok = await pm.apply_agent_tighten_sl(pos, 90.0)  # looser than 95
        assert ok is False
        pm._connector.set_tpsl.assert_not_called()
        assert pos.current_sl_price == 95.0

    @pytest.mark.asyncio
    async def test_rejects_equal_price(self):
        pm = _agent_pm()
        pos = _trade(current_sl_price=95.0)
        ok = await pm.apply_agent_tighten_sl(pos, 95.0)
        assert ok is False

    @pytest.mark.asyncio
    async def test_accepts_tightening(self):
        pm = _agent_pm()
        pos = _trade(current_sl_price=95.0)
        ok = await pm.apply_agent_tighten_sl(pos, 97.0)
        assert ok is True
        assert pos.current_sl_price == 97.0
        pm._connector.set_tpsl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_falls_back_to_computed_sl_when_unset(self):
        """No current_sl_price recorded yet (old row) → derive from entry/config."""
        pm = _agent_pm()
        pos = _trade(current_sl_price=None, entry_price=100.0)  # computed SL = 95.0 (5%)
        ok = await pm.apply_agent_tighten_sl(pos, 94.0)  # looser than computed 95.0
        assert ok is False

    @pytest.mark.asyncio
    async def test_exchange_failure_does_not_update_state(self):
        pm = _agent_pm()
        pm._connector.set_tpsl.side_effect = Exception("boom")
        pos = _trade(current_sl_price=95.0)
        ok = await pm.apply_agent_tighten_sl(pos, 97.0)
        assert ok is False
        assert pos.current_sl_price == 95.0


# ---------------------------------------------------------------------------
# apply_agent_hold_extension — per-call and cumulative caps
# ---------------------------------------------------------------------------


class TestApplyAgentHoldExtension:
    @pytest.mark.asyncio
    async def test_extends_within_budget(self):
        pm = _agent_pm(max_hold_extension_hours=12.0, max_hold_extension_total_hours=24.0)
        pos = _trade()
        ok = await pm.apply_agent_hold_extension(pos, 10.0)
        assert ok is True
        assert pos.llm_hold_extension_total_hours == pytest.approx(10.0)
        expected_deadline = pos.entry_time.replace(tzinfo=timezone.utc) + timedelta(hours=48 + 10)
        assert abs((pos.llm_hold_until - expected_deadline).total_seconds()) < 2

    @pytest.mark.asyncio
    async def test_clamps_to_per_call_max(self):
        pm = _agent_pm(max_hold_extension_hours=5.0, max_hold_extension_total_hours=24.0)
        pos = _trade()
        await pm.apply_agent_hold_extension(pos, 100.0)  # requested way more than per-call cap
        assert pos.llm_hold_extension_total_hours == pytest.approx(5.0)

    @pytest.mark.asyncio
    async def test_cumulative_cap_across_calls(self):
        pm = _agent_pm(max_hold_extension_hours=12.0, max_hold_extension_total_hours=15.0)
        pos = _trade()
        await pm.apply_agent_hold_extension(pos, 10.0)
        ok2 = await pm.apply_agent_hold_extension(pos, 10.0)  # only 5h budget left
        assert ok2 is True
        assert pos.llm_hold_extension_total_hours == pytest.approx(15.0)

    @pytest.mark.asyncio
    async def test_rejects_once_budget_exhausted(self):
        pm = _agent_pm(max_hold_extension_hours=12.0, max_hold_extension_total_hours=10.0)
        pos = _trade()
        await pm.apply_agent_hold_extension(pos, 10.0)
        ok2 = await pm.apply_agent_hold_extension(pos, 1.0)
        assert ok2 is False
        assert pos.llm_hold_extension_total_hours == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# _check_time_exit — respects llm_hold_until (extension only, never shrinks)
# ---------------------------------------------------------------------------


class TestCheckTimeExitWithAgentExtension:
    @pytest.mark.asyncio
    async def test_extension_prevents_premature_close(self):
        pm = _agent_pm()
        now = datetime.now(tz=timezone.utc)
        pos = _trade(
            entry_time=now - timedelta(hours=50),  # past the mechanical 48h deadline
            llm_hold_until=now + timedelta(hours=1),  # agent extended it further
        )
        closed: list = []
        handled = await pm._check_time_exit(pos, now, 100.0, closed)
        assert handled is False
        assert closed == []
        pm._connector.close_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_extension_closes_as_before(self):
        pm = _agent_pm()
        now = datetime.now(tz=timezone.utc)
        pos = _trade(entry_time=now - timedelta(hours=50), llm_hold_until=None)
        closed: list = []
        handled = await pm._check_time_exit(pos, now, 100.0, closed)
        assert handled is True
        assert closed == [pos]
        pm._connector.close_position.assert_awaited_once()


# ---------------------------------------------------------------------------
# apply_agent_close
# ---------------------------------------------------------------------------


class TestApplyAgentClose:
    @pytest.mark.asyncio
    async def test_closes_and_cancels_orders(self):
        pm = _agent_pm()
        pos = _trade(entry_price=100.0, quantity=1.0)
        ok = await pm.apply_agent_close(AsyncMock(), pos, current_price=105.0)
        assert ok is True
        pm._connector.cancel_all_orders.assert_awaited_once()
        pm._connector.close_position.assert_awaited_once()
        assert pos.status == "closed"
        assert pos.exit_price == 105.0

    @pytest.mark.asyncio
    async def test_close_failure_returns_false(self):
        pm = _agent_pm()
        pm._connector.close_position.side_effect = Exception("boom")
        pos = _trade()
        ok = await pm.apply_agent_close(AsyncMock(), pos, current_price=105.0)
        assert ok is False
        assert pos.status == "open"


# ---------------------------------------------------------------------------
# Source scoping — algo and agent pipelines don't see each other's trades
# ---------------------------------------------------------------------------


class TestSourceScoping:
    @pytest.mark.asyncio
    async def test_count_open_scoped_by_source(self, session):
        session.add(_trade(symbol="A/USDT:USDT", source="algo"))
        session.add(_trade(symbol="B/USDT:USDT", source="agent"))
        session.add(_trade(symbol="C/USDT:USDT", source="agent"))
        await session.commit()

        algo_pm = PositionManager(config=_trading_config(), source="algo")
        agent_pm = PositionManager(config=_trading_config(), source="agent")

        assert await algo_pm._count_open(session) == 1
        assert await agent_pm._count_open(session) == 2

    @pytest.mark.asyncio
    async def test_has_position_scoped_by_source(self, session):
        session.add(_trade(symbol="SAME/USDT:USDT", source="algo"))
        await session.commit()

        algo_pm = PositionManager(config=_trading_config(), source="algo")
        agent_pm = PositionManager(config=_trading_config(), source="agent")

        assert await algo_pm._has_position(session, "SAME/USDT:USDT") is True
        # Agent pipeline is on a separate account — an algo trade on the same
        # symbol must never block the agent from trading it independently.
        assert await agent_pm._has_position(session, "SAME/USDT:USDT") is False


# ---------------------------------------------------------------------------
# build_strategy_briefing — the subagent must see real config values, not
# generic text (this is what scripts/agent_briefing.py prints)
# ---------------------------------------------------------------------------


class TestBuildStrategyBriefing:
    def test_reflects_real_config_values(self):
        tc = _trading_config(stop_loss_pct=7.5, risk_reward_ratio=2.5, leverage=8, max_hold_hours=36.0)
        sc = StrategyConfig(volume_surge_mult=4.2, sustain_bars=6, oi_slope_min_pct=3.3)
        briefing = build_strategy_briefing(sc, tc)
        assert "7.5" in briefing
        assert "2.5" in briefing
        assert "8x" in briefing
        assert "x4.2" in briefing
        assert "6 свечей" in briefing

    def test_survives_missing_configs(self):
        briefing = build_strategy_briefing(None, None)
        assert "Circuit Breaker" not in briefing
        assert "известная проблема" in briefing.lower()

    def test_mentions_pullback_only_when_enabled(self):
        tc_off = _trading_config(pending_entry_pullback_pct=0.0)
        tc_on = _trading_config(pending_entry_pullback_pct=1.5, pending_entry_timeout_minutes=9.0)
        assert "откате" not in build_strategy_briefing(None, tc_off)
        assert "откате 1.5%" in build_strategy_briefing(None, tc_on)


# ---------------------------------------------------------------------------
# AgentToolkit — market context from DB snapshot, recent agent decisions
# (the tools scripts/agent_data.py exposes to entry-agent/reeval-agent)
# ---------------------------------------------------------------------------


class TestAgentToolkitMarketContext:
    @pytest.mark.asyncio
    async def test_returns_error_without_snapshot(self, session):
        toolkit = AgentToolkit(session=session, connector=MagicMock(exchange_id="bybit"))
        result = await toolkit.dispatch("get_market_context", {})
        assert "error" in result

    @pytest.mark.asyncio
    async def test_reads_latest_snapshot_from_db(self, session):
        session.add(MarketContextSnapshot(
            timestamp=datetime.now(tz=timezone.utc),
            regime="cautious", regime_start=datetime.now(tz=timezone.utc),
            trend="neutral", trend_start=datetime.now(tz=timezone.utc),
            supertrend_color="red", btc_change_1h=-0.5, btc_change_4h=-1.2,
            others_value=1e9, others_change_1h=-0.3, others_change_4h=-0.8,
            ready=True,
        ))
        await session.commit()

        toolkit = AgentToolkit(session=session, connector=MagicMock(exchange_id="bybit"))
        result = await toolkit.dispatch("get_market_context", {})
        assert result["regime"] == "cautious"
        assert result["supertrend_color"] == "red"
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_ignores_not_ready_snapshot(self, session):
        session.add(MarketContextSnapshot(
            timestamp=datetime.now(tz=timezone.utc),
            regime="unknown", regime_start=datetime.now(tz=timezone.utc),
            trend="neutral", trend_start=datetime.now(tz=timezone.utc),
            supertrend_color="red", btc_change_1h=0.0, btc_change_4h=0.0,
            others_value=0.0, others_change_1h=0.0, others_change_4h=0.0,
            ready=False,
        ))
        await session.commit()

        toolkit = AgentToolkit(session=session, connector=MagicMock(exchange_id="bybit"))
        result = await toolkit.dispatch("get_market_context", {})
        assert "error" in result


class TestAgentToolkitRecentAgentDecisions:
    @pytest.mark.asyncio
    async def test_returns_past_reeval_decisions_for_trade(self, session):
        trade = _trade()
        session.add(trade)
        await session.flush()

        session.add(AgentDecision(
            timestamp=datetime.now(tz=timezone.utc), kind="reeval", trade_id=trade.id,
            symbol=trade.symbol, verdict="hold", reasoning="looked fine",
            applied=False, model="sonnet", agent_version="v2-orchestrator",
        ))
        session.add(AgentDecision(
            timestamp=datetime.now(tz=timezone.utc), kind="reeval", trade_id=trade.id,
            symbol=trade.symbol, verdict="tighten_sl", reasoning="resistance nearby",
            applied=True, model="sonnet", agent_version="v2-orchestrator",
        ))
        await session.commit()

        toolkit = AgentToolkit(session=session, connector=MagicMock(exchange_id="bybit"))
        result = await toolkit.dispatch("get_recent_agent_decisions", {"trade_id": trade.id})
        assert len(result["past_decisions"]) == 2
        verdicts = {d["verdict"] for d in result["past_decisions"]}
        assert verdicts == {"hold", "tighten_sl"}

    @pytest.mark.asyncio
    async def test_empty_for_trade_with_no_history(self, session):
        trade = _trade()
        session.add(trade)
        await session.flush()

        toolkit = AgentToolkit(session=session, connector=MagicMock(exchange_id="bybit"))
        result = await toolkit.dispatch("get_recent_agent_decisions", {"trade_id": trade.id})
        assert result["past_decisions"] == []
