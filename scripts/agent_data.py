"""CLI-обёртка над AgentToolkit — единственный источник данных для сабагентов
ИИ-режима (entry-agent/reeval-agent). Только чтение, никаких побочных эффектов
на торговлю. Вызывается сабагентами через Bash (единственный разрешённый
инструмент — см. .claude/agents/entry-agent.md, reeval-agent.md).

Usage:
    python scripts/agent_data.py <tool_name> '<json_kwargs>'

Пример:
    python scripts/agent_data.py get_funding_rate '{"symbol": "PEPE/USDT:USDT"}'
    python scripts/agent_data.py get_market_context '{}'
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agent.tools import AgentToolkit
from src.analytics.data_provider import CandleCache
from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.storage.database import async_session

CONFIG_PATH = "config/config.yaml"


async def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"error": "usage: agent_data.py <tool_name> '<json_kwargs>'"}))
        sys.exit(1)

    tool_name = sys.argv[1]
    tool_input = json.loads(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip() else {}

    settings = Settings.from_yaml(CONFIG_PATH)
    agent_cfg = settings.agent
    connector = ExchangeConnector(
        exchange_id=agent_cfg.exchange, api_key=agent_cfg.api_key, secret=agent_cfg.secret,
    )
    try:
        async with async_session() as session:
            toolkit = AgentToolkit(
                session=session,
                connector=connector,
                candle_cache=CandleCache(),
                trading_config=settings.trading,
            )
            result = await toolkit.dispatch(tool_name, tool_input)
            print(json.dumps(result, default=str, ensure_ascii=False))
    finally:
        await connector.close()


if __name__ == "__main__":
    asyncio.run(main())
