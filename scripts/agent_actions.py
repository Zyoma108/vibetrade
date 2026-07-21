"""Единственное место, где решение ИИ-агента реально применяется — открывает
или изменяет сделку на ОТДЕЛЬНОМ аккаунте (source='agent') и пишет строку в
agent_decisions. Вызывается ТОЛЬКО оркестратором (.claude/skills/
vibetrade-agent-loop) после того, как сабагент (entry-agent/reeval-agent)
вынес вердикт — сами сабагенты этот скрипт не вызывают (у них только
scripts/agent_data.py, чтение).

Аргумент — путь к JSON-файлу, не инлайн-JSON (чтобы не ловить проблемы с
экранированием длинного текста reasoning в shell).

Usage:
    python scripts/agent_actions.py open_entry /path/to/decision.json
    python scripts/agent_actions.py tighten_sl /path/to/decision.json
    python scripts/agent_actions.py extend_hold /path/to/decision.json
    python scripts/agent_actions.py close /path/to/decision.json

Форматы decision.json по действиям:
    open_entry:   {"signal_id": int, "approve": bool, "reasoning": str}
    tighten_sl:   {"trade_id": int, "new_sl_price": float, "reasoning": str}
    extend_hold:  {"trade_id": int, "extend_hours": float, "reasoning": str}
    close:        {"trade_id": int, "reasoning": str}
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agent.tools import AGENT_VERSION
from src.analytics.base import Signal
from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.executor.position_manager import PositionManager
from src.storage.database import async_session
from src.storage.models import AgentDecision, Signal as SignalModel, Trade

CONFIG_PATH = "config/config.yaml"


async def _record(session, kind: str, symbol: str, verdict: str, reasoning: str,
                   applied: bool, model: str, signal_id: int | None = None,
                   trade_id: int | None = None) -> None:
    session.add(AgentDecision(
        timestamp=datetime.now(tz=timezone.utc),
        kind=kind,
        signal_id=signal_id,
        trade_id=trade_id,
        symbol=symbol,
        verdict=verdict,
        reasoning=reasoning,
        tool_calls_json=None,  # трейс инструментов уже виден в беседе оркестратора
        applied=applied,
        model=model,
        agent_version=AGENT_VERSION,
        latency_ms=None,
    ))


async def open_entry(session, pm: PositionManager, cfg, payload: dict) -> dict:
    signal_id = payload["signal_id"]
    approve = bool(payload.get("approve", True))
    reasoning = payload.get("reasoning", "")

    db_signal = await session.get(SignalModel, signal_id)
    if not db_signal:
        return {"success": False, "error": f"signal {signal_id} not found"}

    applied = False
    detail = None
    if approve and not cfg.dry_run:
        sig = Signal(
            symbol=db_signal.symbol,
            setup_type=db_signal.setup_type,
            direction=db_signal.direction,
            confidence=db_signal.confidence,
            message=db_signal.message,
        )
        _trade, status, detail = await pm.open_position(session, sig, signal_id=signal_id)
        applied = status in ("opened", "pending")
        if not applied:
            db_signal.missed_reason = status
            db_signal.missed_detail = detail

    await _record(
        session, kind="entry", symbol=db_signal.symbol,
        verdict="approve" if approve else "reject", reasoning=reasoning,
        applied=applied, model=cfg.model, signal_id=signal_id,
    )
    return {"success": True, "applied": applied, "detail": detail}


async def tighten_sl(session, pm: PositionManager, cfg, payload: dict) -> dict:
    trade_id = payload["trade_id"]
    new_sl_price = payload["new_sl_price"]
    reasoning = payload.get("reasoning", "")

    trade = await session.get(Trade, trade_id)
    if not trade:
        return {"success": False, "error": f"trade {trade_id} not found"}

    applied = False
    if not cfg.dry_run and cfg.allow_sl_tighten:
        applied = await pm.apply_agent_tighten_sl(trade, new_sl_price)

    await _record(
        session, kind="reeval", symbol=trade.symbol, verdict="tighten_sl",
        reasoning=reasoning, applied=applied, model=cfg.model, trade_id=trade_id,
    )
    return {"success": True, "applied": applied}


async def extend_hold(session, pm: PositionManager, cfg, payload: dict) -> dict:
    trade_id = payload["trade_id"]
    extend_hours = payload["extend_hours"]
    reasoning = payload.get("reasoning", "")

    trade = await session.get(Trade, trade_id)
    if not trade:
        return {"success": False, "error": f"trade {trade_id} not found"}

    applied = False
    if not cfg.dry_run:
        applied = await pm.apply_agent_hold_extension(trade, extend_hours)

    await _record(
        session, kind="reeval", symbol=trade.symbol, verdict="extend_hold",
        reasoning=reasoning, applied=applied, model=cfg.model, trade_id=trade_id,
    )
    return {"success": True, "applied": applied}


async def close(session, pm: PositionManager, cfg, payload: dict) -> dict:
    trade_id = payload["trade_id"]
    reasoning = payload.get("reasoning", "")

    trade = await session.get(Trade, trade_id)
    if not trade:
        return {"success": False, "error": f"trade {trade_id} not found"}

    applied = False
    if not cfg.dry_run and cfg.allow_early_close:
        current_price = await pm._get_current_price(session, trade.symbol)
        applied = await pm.apply_agent_close(session, trade, current_price)

    await _record(
        session, kind="reeval", symbol=trade.symbol, verdict="close",
        reasoning=reasoning, applied=applied, model=cfg.model, trade_id=trade_id,
    )
    return {"success": True, "applied": applied}


ACTIONS = {
    "open_entry": open_entry,
    "tighten_sl": tighten_sl,
    "extend_hold": extend_hold,
    "close": close,
}


async def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in ACTIONS:
        print(json.dumps({"error": f"usage: agent_actions.py <{'|'.join(ACTIONS)}> <path/to/decision.json>"}))
        sys.exit(1)

    action_name = sys.argv[1]
    payload = json.loads(Path(sys.argv[2]).read_text())

    settings = Settings.from_yaml(CONFIG_PATH)
    agent_cfg = settings.agent
    connector = ExchangeConnector(
        exchange_id=agent_cfg.exchange, api_key=agent_cfg.api_key, secret=agent_cfg.secret,
    )
    pm = PositionManager(
        config=settings.trading, trading_connector=connector, source="agent", agent_config=agent_cfg,
    )
    try:
        async with async_session() as session:
            result = await ACTIONS[action_name](session, pm, agent_cfg, payload)
            await session.commit()
            print(json.dumps(result, default=str, ensure_ascii=False))
    finally:
        await connector.close()


if __name__ == "__main__":
    asyncio.run(main())
