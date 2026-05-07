import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ExchangeConfig(BaseModel):
    enabled: bool = True


class CollectorsConfig(BaseModel):
    interval_seconds: int = Field(default=60, ge=10)


class StrategyConfig(BaseModel):
    min_volume_usdt: float = 100_000
    oi_change_threshold_pct: float = 5.0


class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_ids: list[int] = []


class TradingConfig(BaseModel):
    mode: str = "signal"


class Settings(BaseModel):
    exchanges: dict[str, ExchangeConfig]
    coins: list[str]
    collectors: CollectorsConfig = CollectorsConfig()
    strategy: StrategyConfig = StrategyConfig()
    telegram: TelegramConfig = TelegramConfig()
    trading: TradingConfig = TradingConfig()

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        raw = Path(path).read_text()
        raw = cls._substitute_env(raw)
        data = yaml.safe_load(raw)
        return cls(**data)

    @staticmethod
    def _substitute_env(raw: str) -> str:
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), raw)
