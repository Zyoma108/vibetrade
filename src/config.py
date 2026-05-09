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
    min_volume_usdt: float = 200_000
    exclude_coins: list[str] = Field(
        default=["BTC", "ETH"],
        description="Монеты, исключаемые из сканирования (без /USDT)",
    )
    volume_surge_mult: float = Field(
        default=2.0, description="Во сколько раз объём должен превышать норму"
    )
    sustain_bars: int = Field(
        default=4, description="Сколько свечей подряд должны быть выше порога"
    )
    oi_change_threshold_pct: float = 5.0


class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_ids: list[str] = []  # числовые ID, @username канала, или отрицательные ID


class TradingConfig(BaseModel):
    mode: str = "signal"              # signal | virtual | real
    max_positions: int = Field(default=10, ge=1, description="Максимум одновременных позиций")
    position_size_usdt: float = Field(default=100.0, ge=10, description="Размер позиции в USDT")
    take_profit_pct: float = Field(default=12.0, ge=0.5, description="Тейк-профит, %")
    stop_loss_pct: float = Field(default=4.0, ge=0.5, description="Стоп-лосс, %")


class Settings(BaseModel):
    exchanges: dict[str, ExchangeConfig]
    coins: list[str] = []  # пустой список = сканировать все монеты динамически
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
