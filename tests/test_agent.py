"""
Tests for the AI-mode agent subsystem: PositionManager's agent-facing methods
(tighten SL, extend hold, early close, source scoping) and DecisionAgent's
tool-calling loop (fail-open/fail-safe behavior, termination conditions).

Uses minimal mocking, mirrors the conventions in test_position_manager.py.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.agent.decision_agent import DecisionAgent
from src.analytics.base import Signal
from src.config import AgentConfig, StrategyConfig, TradingConfig
from src.executor.position_manager import PositionManager
from src.storage.models import Base, Trade

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


def _signal(symbol: str = "TEST/USDT:USDT") -> Signal:
    return Signal(symbol=symbol, setup_type="volume_surge", direction="long", confidence=80, message="Test signal")


def _tool_use(name: str, input_: dict, id_: str = "tu_1"):
    return SimpleNamespace(type="tool_use", name=name, input=input_, id=id_)


def _text(text: str = "thinking"):
    return SimpleNamespace(type="text", text=text)


def _response(content: list):
    return SimpleNamespace(content=content)


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
# DecisionAgent — tool-calling loop: fail-open/fail-safe, termination
# ---------------------------------------------------------------------------


def _agent(config: AgentConfig | None = None) -> DecisionAgent:
    """DecisionAgent with enabled=False so __init__ skips real Anthropic client
    construction — tests inject a fake client afterwards."""
    cfg = config or _agent_config(enabled=False)
    agent = DecisionAgent(config=cfg, connector=MagicMock(exchange_id="bybit"))
    return agent


class TestDecisionAgentEntryLoop:
    @pytest.mark.asyncio
    async def test_approves_on_immediate_final_call(self):
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            return_value=_response([_tool_use("submit_entry_decision", {"approve": True, "reasoning": "looks fine"})])
        )
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True
        assert verdict.reasoning == "looks fine"
        assert verdict.failed is False

    @pytest.mark.asyncio
    async def test_rejects_when_model_says_no(self):
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            return_value=_response([_tool_use("submit_entry_decision", {"approve": False, "reasoning": "overheated"})])
        )
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is False
        assert verdict.reasoning == "overheated"

    @pytest.mark.asyncio
    async def test_fail_open_on_timeout(self):
        agent = _agent(_agent_config(enabled=False, decision_timeout_seconds=5.0))
        agent.config.decision_timeout_seconds = 0.05  # bypass pydantic ge=5 floor for a fast test

        async def _slow(**kwargs):
            import asyncio
            await asyncio.sleep(1.0)

        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(side_effect=_slow)
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True  # fail-open
        assert verdict.failed is True

    @pytest.mark.asyncio
    async def test_fail_open_on_exception(self):
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(side_effect=RuntimeError("api down"))
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True
        assert verdict.failed is True

    @pytest.mark.asyncio
    async def test_fail_open_when_tool_call_budget_exceeded(self):
        """Model keeps calling data tools and never submits a final decision."""
        agent = _agent(_agent_config(enabled=False, max_tool_calls_per_decision=2))
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            return_value=_response([_tool_use("get_market_context", {})])
        )
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True  # fail-open
        assert verdict.failed is True

    @pytest.mark.asyncio
    async def test_fail_open_when_model_returns_no_tool_use(self):
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(return_value=_response([_text("I dunno")]))
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True
        assert verdict.failed is True

    @pytest.mark.asyncio
    async def test_dispatches_data_tool_before_final_decision(self):
        """First turn requests a data tool, second turn submits the decision."""
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            side_effect=[
                _response([_tool_use("get_market_context", {}, id_="tu_1")]),
                _response([_tool_use("submit_entry_decision", {"approve": True, "reasoning": "ok"}, id_="tu_2")]),
            ]
        )
        verdict = await agent.evaluate_entry(MagicMock(), _signal())
        assert verdict.approved is True
        assert len(verdict.tool_trace) == 1
        assert verdict.tool_trace[0]["tool"] == "get_market_context"
        assert agent._client.messages.create.await_count == 2


class TestDecisionAgentReevalLoop:
    @pytest.mark.asyncio
    async def test_fail_safe_holds_on_timeout(self):
        agent = _agent(_agent_config(enabled=False, decision_timeout_seconds=5.0))
        agent.config.decision_timeout_seconds = 0.05  # bypass pydantic ge=5 floor for a fast test

        async def _slow(**kwargs):
            import asyncio
            await asyncio.sleep(1.0)

        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(side_effect=_slow)
        verdict = await agent.evaluate_position(MagicMock(), _trade())
        assert verdict.action == "hold"
        assert verdict.failed is True

    @pytest.mark.asyncio
    async def test_returns_tighten_sl_action(self):
        agent = _agent()
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            return_value=_response([
                _tool_use("submit_reeval_decision", {
                    "action": "tighten_sl", "reasoning": "resistance nearby", "new_sl_price": 101.0,
                })
            ])
        )
        verdict = await agent.evaluate_position(MagicMock(), _trade())
        assert verdict.action == "tighten_sl"
        assert verdict.new_sl_price == 101.0

    @pytest.mark.asyncio
    async def test_daily_budget_exhausted_fails_safe(self):
        agent = _agent(_agent_config(enabled=False, daily_call_budget=1))
        agent._client = MagicMock()
        agent._client.messages.create = AsyncMock(
            return_value=_response([_tool_use("submit_reeval_decision", {"action": "hold", "reasoning": "ok"})])
        )
        v1 = await agent.evaluate_position(MagicMock(), _trade())
        assert v1.failed is False
        v2 = await agent.evaluate_position(MagicMock(), _trade())
        assert v2.failed is True
        assert v2.action == "hold"


# ---------------------------------------------------------------------------
# Strategy briefing — the model must see real config values, not generic text
# ---------------------------------------------------------------------------


class TestStrategyBriefing:
    def test_reflects_real_config_values(self):
        tc = _trading_config(stop_loss_pct=7.5, risk_reward_ratio=2.5, leverage=8, max_hold_hours=36.0)
        sc = StrategyConfig(volume_surge_mult=4.2, sustain_bars=6, oi_slope_min_pct=3.3)
        agent = DecisionAgent(
            config=_agent_config(enabled=False),
            connector=MagicMock(exchange_id="bybit"),
            trading_config=tc,
            strategy_config=sc,
        )
        briefing = agent._strategy_briefing
        assert "7.5" in briefing
        assert "2.5" in briefing
        assert "8x" in briefing
        assert "x4.2" in briefing
        assert "6 свечей" in briefing

    def test_survives_missing_configs(self):
        """No trading_config/strategy_config passed (e.g. old caller) — must not crash."""
        agent = DecisionAgent(config=_agent_config(enabled=False), connector=MagicMock(exchange_id="bybit"))
        assert "Circuit Breaker" not in agent._strategy_briefing
        assert "известная проблема" in agent._strategy_briefing.lower()

    @pytest.mark.asyncio
    async def test_included_in_system_prompt_sent_to_model(self):
        tc = _trading_config(stop_loss_pct=6.0)
        agent = DecisionAgent(
            config=_agent_config(enabled=False), connector=MagicMock(exchange_id="bybit"), trading_config=tc,
        )
        agent._client = MagicMock()
        create = AsyncMock(
            return_value=_response([_tool_use("submit_entry_decision", {"approve": True, "reasoning": "ok"})])
        )
        agent._client.messages.create = create
        await agent.evaluate_entry(MagicMock(), _signal())
        sent_system_prompt = create.call_args.kwargs["system"]
        assert "6.0" in sent_system_prompt
