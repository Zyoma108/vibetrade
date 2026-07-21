import os
import re
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


class ExchangeConfig(BaseModel):
    enabled: bool = True
    api_key: str = ""
    secret: str = ""


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
        default=1.0, description="Минимальный рост цены за sustain-окно, %"
    )
    price_growth_max_pct: float = Field(
        default=12.0, description="Максимальный рост цены за sustain-окно, % (страховочный потолок, 0 = без лимита)"
    )
    exhaustion_gain_pct: float = Field(
        default=5.0, description="Порог роста цены в % для exhaustion-фильтра (срабатывает вместе с exhaustion_pos_ratio)"
    )
    exhaustion_pos_ratio: float = Field(
        default=0.7, description="Позиция закрытия последней свечи (0=low, 1=high), выше которой + exhaustion_gain = сигнал истощения"
    )
    max_hourly_drop_pct: float = Field(
        default=10.0, description="Максимальное падение за час, % (защита от рагпулов, 0 = выкл)"
    )
    pre_surge_max_pct: float = Field(
        default=0.0, description="Максимальный рост за 30 мин до sustain-окна, % (0 = выкл)"
    )
    dump_volume_mult: float = Field(
        default=3.0, description="Макс. отношение объёма последней свечи к медиане остальных свечей sustain-окна (защита от свечей-выбросов, 0 = выкл)"
    )
    smooth_max_ratio: float = Field(
        default=5.0, description="Макс. отношение макс/медиана объёма в окне (отсекает спайки, уменьшить для более жёсткого фильтра)"
    )
    # Параметры для PriceSurgeDetector (strategy_price_surge)
    price_surge_pct: float = Field(
        default=0.0, description="Рост цены для сигнала пампа, % (0 = детектор выключен)"
    )
    price_surge_minutes: int = Field(
        default=9, description="Промежуток времени для замера роста цены, минут"
    )
    # Множитель volume_surge_mult для CAUTIOUS режима рынка
    cautious_volume_surge_mult_increase_pct: float = Field(
        default=50.0, ge=0.0, le=200.0,
        description="На сколько % увеличить volume_surge_mult в CAUTIOUS режиме (0 = без изменений)"
    )


class TelegramConfig(BaseModel):
    bot_token: str = ""
    chat_ids: list[str] = []  # числовые ID, @username канала, или отрицательные ID


class TradingConfig(BaseModel):
    mode: str = "signal"              # signal | real
    exchange: str = "bybit"           # биржа для торговли
    max_positions: int = Field(default=10, ge=1, description="Максимум одновременных позиций")
    leverage: int = Field(default=1, ge=1, le=100, description="Кредитное плечо")
    risk_per_trade_pct: float = Field(default=1.0, ge=0.1, le=100.0, description="% от депозита, которым рискуем за один стоп")
    risk_reward_ratio: float = Field(default=3.0, ge=1.0, le=20.0, description="Соотношение TP/SL (3.0 = 1:3 risk/reward)")
    stop_loss_pct: float = Field(default=5.0, ge=0.5, le=50.0, description="Стоп-лосс, % от цены входа")
    max_hold_hours: float = Field(default=24.0, ge=1.0, description="Максимальное время удержания позиции, часов")
    partial_close_pct: float = Field(default=50.0, ge=10.0, le=90.0, description="% пути до TP для частичного закрытия / перевода в б/у")
    cooldown_hours: float = Field(default=1.0, ge=0.0, le=168.0, description="Кулдаун после закрытия позиции, часов (0 = без кулдауна)")
    circuit_breaker_enabled: bool = Field(default=True, description="Включить защиту от серий убытков (Circuit Breaker)")
    circuit_breaker_loss_streak_reduce: int = Field(default=3, ge=1, le=20, description="После скольких убытков подряд уменьшить размер позиции")
    circuit_breaker_reduce_mult_pct: float = Field(default=50.0, ge=10.0, le=90.0, description="Множитель размера позиции при срабатывании, %")
    circuit_breaker_loss_streak_stop: int = Field(default=5, ge=1, le=50, description="После скольких убытков подряд полностью остановить торговлю")
    circuit_breaker_stop_minutes: int = Field(default=60, ge=10, le=1440, description="На сколько минут остановить торговлю при полном срабатывании")
    taker_fee_pct: float = Field(default=0.055, ge=0.0, le=1.0, description="Комиссия тейкера (market-ордер), % от notional (Bybit VIP0 по умолчанию)")
    maker_fee_pct: float = Field(default=0.02, ge=0.0, le=1.0, description="Комиссия мейкера (лимитный reduce-only ордер), % от notional (Bybit VIP0 по умолчанию)")
    backtest_slippage_pct: float = Field(default=0.3, ge=0.0, le=5.0, description="Допущение на проскальзывание входа в бэктесте, % (0 = выкл). Бэктест иначе входит по цене закрытия свечи, что оптимистичнее реального market-ордера")


class MarketContextConfig(BaseModel):
    enabled: bool = Field(default=True, description="Включить рыночный контекст (BTC + OTHERS Supertrend)")
    btc_drop_threshold_pct: float = Field(default=1.5, description="Порог падения BTC за час для cautious/risk-off, %")
    trend_threshold_pct: float = Field(default=1.0, ge=0.1, le=20.0, description="Порог изменения цены за 4 часа для определения тренда (bullish/bearish), %")
    supertrend_atr_period: int = Field(default=10, ge=3, le=50, description="Период ATR для Supertrend")
    supertrend_multiplier: float = Field(default=3.0, ge=1.0, le=10.0, description="Множитель ATR для Supertrend")


class Settings(BaseModel):
    exchanges: dict[str, ExchangeConfig]
    collectors: CollectorsConfig = CollectorsConfig()
    strategy: StrategyConfig = StrategyConfig()
    strategy_price_surge: Optional[StrategyConfig] = None   # вторая стратегия (только сигналы, без торговли)
    telegram: TelegramConfig = TelegramConfig()
    telegram_price_surge: Optional[TelegramConfig] = None   # отдельный бот для сигналов strategy_price_surge
    trading: TradingConfig = TradingConfig()
    market_context: MarketContextConfig = MarketContextConfig()

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        raw = Path(path).read_text()
        raw = cls._substitute_env(raw)
        data = yaml.safe_load(raw)
        return cls(**data)

    @staticmethod
    def _substitute_env(raw: str) -> str:
        return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), raw)
