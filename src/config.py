import os
import re
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


class ExchangeConfig(BaseModel):
    enabled: bool = True
    api_key: str = ""
    secret: str = ""
    testnet: bool = False


class CollectorsConfig(BaseModel):
    interval_seconds: int = Field(default=60, ge=10)
    timeframe: str = Field(default="5m", description="Таймфрейм свечей (1m, 5m, 15m, 1h)")


class StrategyConfig(BaseModel):
    min_volume_usdt: float = 200_000
    exclude_coins: list[str] = Field(
        default=["BTC", "ETH"],
        description="Монеты, исключаемые из сканирования (без /USDT)",
    )
    baseline_bars: int = Field(
        default=50, description="Свечей для расчёта нормального объёма"
    )
    volume_surge_mult: float = Field(
        default=2.0, description="Во сколько раз объём должен превышать норму"
    )
    min_baseline_volume_usdt: float = Field(
        default=0.0, description="Минимальная медиана объёма в USDT, 0 = фильтр выключен"
    )
    sustain_bars: int = Field(
        default=4, description="Сколько свечей подряд должны быть выше порога"
    )
    oi_slope_min_pct: float = Field(
        default=2.0, description="Минимальный наклон OI, % (фильтрует плоский/падающий OI)"
    )
    price_growth_min_pct: float = Field(
        default=0.3, description="Минимальный рост цены за sustain-окно, %"
    )
    price_growth_max_pct: float = Field(
        default=0.0, description="Максимальный рост цены за sustain-окно, % (0 = без лимита)"
    )
    max_hourly_drop_pct: float = Field(
        default=10.0, description="Максимальное падение за час, % (защита от рагпулов, 0 = выкл)"
    )


class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_ids: list[str] = []  # числовые ID, @username канала, или отрицательные ID


class TradingConfig(BaseModel):
    mode: str = "signal"              # signal | virtual | real
    exchange: str = "bybit"           # биржа для торговли
    max_positions: int = Field(default=10, ge=1, description="Максимум одновременных позиций")
    position_size_usdt: float = Field(default=100.0, ge=10, description="Объём позиции в USDT (не маржа)")
    position_size_pct: float = Field(default=0.0, ge=0.0, le=100.0, description="% от депозита на позицию (0 = использовать position_size_usdt)")
    leverage: int = Field(default=1, ge=1, le=100, description="Кредитное плечо")
    take_profit_pct: float = Field(default=12.0, ge=0.5, description="Тейк-профит, %")
    stop_loss_pct: float = Field(default=4.0, ge=0.5, description="Стоп-лосс, %")
    max_hold_hours: float = Field(default=24.0, ge=1.0, description="Максимальное время удержания позиции, часов")
    partial_close_enabled: bool = Field(default=False, description="Частичная фиксация на полпути к TP")
    partial_close_pct: float = Field(default=50.0, ge=10.0, le=90.0, description="% пути до TP для частичного закрытия / перевода в б/у")
    breakeven_at_halfway: bool = Field(default=False, description="Перевести стоп в б/у на полпути (без частичной фиксации)")


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
