"""Печатает динамический strategy briefing для сабагентов ИИ-режима — реальные
пороги детектора и риск-параметры аккаунта из живого config.yaml (не хардкод
текстом, иначе разойдётся при правке конфига). Оркестратор вызывает это раз за
цикл и вставляет вывод текстом в промпт entry-agent/reeval-agent.

Usage:
    python scripts/agent_briefing.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agent.tools import build_strategy_briefing
from src.config import Settings

CONFIG_PATH = "config/config.yaml"


def main() -> None:
    settings = Settings.from_yaml(CONFIG_PATH)
    print(build_strategy_briefing(settings.strategy, settings.trading))


if __name__ == "__main__":
    main()
