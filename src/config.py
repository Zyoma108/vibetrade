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
    fetch_concurrency: int = Field(
        default=20, ge=1, le=100,
        description="Сколько сетевых вызовов к одной бирже держим в полёте "
        "одновременно в цикле сбора. Было захардкожено 5 — это ~116 "
        "последовательных раундов на 580 монет, то есть цикл равен "
        "116 x латентность. Замер 01.09.2026 на публичном API bybit: "
        "5 -> 11.8 req/s, 20 -> 34 req/s. Поднимать выше пула потоков "
        "бессмысленно — пул выставляется по этому же числу, см. "
        "Application._setup_thread_pool",
    )
    timeframe: str = Field(default="5m", description="Таймфрейм свечей (1m, 5m, 15m, 1h)")
    retention_days: int = Field(
        default=30, ge=0,
        description="Сколько суток истории свечей и OI хранить (0 = хранить всё). "
        "До 01.09.2026 удаления не было вообще: за 5 суток БД набирала 459 МБ, "
        "архивная за 15 суток весит 3.2 ГБ. Детектору нужны 84 бара на монету, "
        "остальное лежит мёртвым грузом и удорожает вставку (каждая новая строка "
        "правит B-деревья индексов). Бэктестам 30 суток хватает с запасом — "
        "все окна аудитов были 5-15 суток; для более длинных прогонов копию БД "
        "нужно откладывать в архив заранее.",
    )
    scan_cycle_seconds: float = Field(
        default=105.0, ge=1.0,
        description="Измеренная длительность полного цикла сканирования рынка "
        "(сбор+анализ всех символов + interval_seconds паузы) — реальный каданс, с "
        "которым бот ищет новые сигналы. Используется только бэктестом "
        "(src/backtest/runner.py, scripts/sweep_retracement.py), чтобы не проверять "
        "сигналы на каждом баре, а сэмплировать их так же редко/часто, как это "
        "реально происходит. Не читается MarketDataCollector'ом (тот таймингует себя "
        "сам через interval_seconds) — обновлять вручную при изменении скорости "
        "скана (см. память market-data-scan-speedup-august-2026: ~90с после "
        "оптимизации 20.08.2026, было 5-7 мин)"
    )


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
    oi_filter_enabled: bool = Field(
        default=True,
        description="Включить OI-подтверждение (oi_declining + oi_slope_min_pct). "
        "Аудит августа 2026 (15 дней, 5540 кандидатов, walk-forward в обе стороны): весь блок "
        "net-вреден. OI собирается раз в цикл сканирования, поэтому OI_TREND_BARS=3 покрывает "
        "не фиксированное окно, а 4-10 минут в зависимости от скорости скана; требование "
        "«+2% OI за этот срок» механически отбирает уже перегретые сетапы — у прошедших гейт "
        "медианный размах sustain-окна 4.05% против 1.65% у отсеянных, pump-от-baseline 6.34% "
        "против 2.0%, TP 16.8% против 22-24%, SL 36.1% против 24%. Выключение гейта меняет "
        "мат. ожидание с -0.089R на +0.146R и утраивает число сигналов (180 -> 600). "
        "OI продолжает собираться в БД — выключен только гейт."
    )
    oi_slope_min_pct: float = Field(
        default=2.0, description="Минимальный наклон OI, % (активен только при oi_filter_enabled)"
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
    exhaustion_extreme_pct: float = Field(
        default=30.0, ge=0.0, le=200.0,
        description="Абсолютный порог экстремального пампа от медианы baseline, % (0 = выкл). "
        "Раньше был захардкожен в детекторе как exhaustion_gain_pct * 6.0 = 30% и на практике "
        "почти никого не отсекал. Аудит августа 2026: по децилям 'pump-от-baseline' оптимум "
        "1.5-5%, а выше 6.8% мат. ожидание уходит в ноль и минус; порог 10% даёт +0.702R против "
        "+0.589R и улучшает обе недели."
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
    max_window_retracement_pct: float = Field(
        default=0.0, ge=0.0, le=100.0,
        description="Макс. допустимый откат цены от пика (high) sustain-окна к моменту закрытия последней свечи, % (0 = выкл). "
        "Ловит случаи, когда движение уже развернулось, пока набиралось подтверждение объёма (см. db-audit-august-2026: "
        "92% живых лоссов на алго-пути ни разу не доходили даже до частичной фиксации +3.5%). "
        "Выключен по умолчанию — включать только после свипа порога на исторических данных (см. AGENTS.md)"
    )
    max_window_range_pct: float = Field(
        default=0.0, ge=0.0, le=100.0,
        description="Макс. размах (high/low - 1) sustain-окна, % (0 = выкл). Стоп-лосс фиксирован "
        "в процентах (stop_loss_pct), поэтому у монеты, которая и так ходит на 4-7% за 12 минут, "
        "он оказывается внутри обычного шума. Аудит августа 2026: сильнейший одиночный предиктор "
        "исхода, монотонный по децилям и устойчивый на обеих неделях — при размахе >4% мат. "
        "ожидание -0.145R, при <=2.5% +0.26R; отбор порога на одной неделе и проверка на другой "
        "подтверждают эффект out-of-sample в обе стороны."
    )
    smooth_max_ratio: float = Field(
        default=5.0, description="Макс. отношение макс/медиана объёма в окне (отсекает спайки, уменьшить для более жёсткого фильтра)"
    )
    # Ниже — пороги, до 26.08.2026 захардкоженные в SetupDetector. Дефолты равны
    # прежним зашитым значениям, поэтому поведение не изменилось; смысл выноса в
    # том, что теперь их можно свипать. В проекте это уже дважды окупалось:
    # exhaustion_extreme перестал быть `ex_gain * 6`, а partial_close_qty_pct —
    # пятьюдесятью процентами (сейчас 30). Аудит фильтров августа 2026 показал,
    # что volume_fading и volume_declining отсекают >50% near-miss без видимого
    # эффекта — проверить это было нечем, пока пороги жили в коде.
    volume_fading_ratio: float = Field(
        default=0.7, ge=0.0, le=1.0,
        description="Мин. отношение объёма последней свечи sustain-окна к среднему предыдущих "
        "(ниже — памп иссякает, сигнал не даём; 0 = выкл). Кандидат на свип: по аудиту "
        "августа 2026 фильтр отсекает много near-miss без подтверждённого эффекта"
    )
    volume_declining_enabled: bool = Field(
        default=True,
        description="Требовать, чтобы объём последней свечи sustain-окна был не ниже первой. "
        "Кандидат на свип вместе с volume_fading_ratio — фильтры меряют близкое и могут дублировать друг друга"
    )
    oi_declining_enabled: bool = Field(
        default=True,
        description="Отбрасывать сигнал, если последняя точка OI ниже предпоследней (приток иссякает). "
        "Действует только при oi_filter_enabled=true. ВАЖНО: именно эта проверка отсутствовала "
        "в свип-скриптах и завысила прошлые свипы по RR/partial-close/retracement"
    )
    pre_surge_bars: int = Field(
        default=10, ge=1, le=100,
        description="Длина окна ПЕРЕД sustain-окном, на котором меряется pre_surge_max_pct, в свечах "
        "(10 свечей = 30 мин на 3m). Раньше было захардкожено"
    )
    confidence_surge_mult: float = Field(
        default=5.0, gt=0.0,
        description="Множитель в формуле confidence = min(surge × N, 100). При volume_surge_mult=5 "
        "и N=5 шкала упирается в потолок уже на surge x20: в БД за 10.08-25.08 у 52% сигналов "
        "confidence=100, то есть для половины популяции метрика не несёт информации "
        "(и наблюдавшийся «худший win rate у confidence=100» — артефакт насыщения)"
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
    # None (или пустая строка) = бот отключён — все места использования уже проверяют
    # bot_token на truthy перед стартом (см. core/app.py), поэтому и `bot_token: null`
    # в YAML, и просто отсутствие ключа — оба штатный способ выключить конкретного бота
    # (telegram: основной, telegram_price_surge: второй, для сигналов памп-стратегии)
    # не трогая остальную секцию (chat_ids и т.д.).
    bot_token: str | None = None
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
    partial_close_qty_pct: float = Field(default=50.0, ge=5.0, le=95.0, description="Какая доля позиции (%) закрывается при срабатывании partial-триггера; остаток идёт в б/у-стоп и бежит на полный TP")
    cooldown_hours: float = Field(default=1.0, ge=0.0, le=168.0, description="Кулдаун после закрытия позиции, часов (0 = без кулдауна)")
    circuit_breaker_enabled: bool = Field(default=True, description="Включить защиту от серий убытков (Circuit Breaker)")
    circuit_breaker_loss_streak_reduce: int = Field(default=3, ge=1, le=20, description="После скольких убытков подряд уменьшить размер позиции")
    circuit_breaker_reduce_mult_pct: float = Field(default=50.0, ge=10.0, le=90.0, description="Множитель размера позиции при срабатывании, %")
    circuit_breaker_loss_streak_stop: int = Field(default=5, ge=1, le=50, description="После скольких убытков подряд полностью остановить торговлю")
    circuit_breaker_stop_minutes: int = Field(default=60, ge=10, le=1440, description="На сколько минут остановить торговлю при полном срабатывании")
    taker_fee_pct: float = Field(default=0.055, ge=0.0, le=1.0, description="Комиссия тейкера (market-ордер), % от notional (Bybit VIP0 по умолчанию)")
    maker_fee_pct: float = Field(default=0.02, ge=0.0, le=1.0, description="Комиссия мейкера (лимитный reduce-only ордер), % от notional (Bybit VIP0 по умолчанию)")
    backtest_slippage_pct: float = Field(default=0.3, ge=0.0, le=5.0, description="Допущение на проскальзывание входа в бэктесте, % (0 = выкл). Бэктест иначе входит по цене закрытия свечи, что оптимистичнее реального market-ордера")
    pending_entry_pullback_pct: float = Field(default=0.0, ge=0.0, le=10.0, description="Вход лимитным ордером на откате от цены сигнала, % (0 = выкл — вход market сразу по сигналу, как раньше). Решает проблему покупки на пике пампа")
    pending_entry_timeout_minutes: float = Field(default=9.0, ge=1.0, le=180.0, description="Через сколько минут снять неисполненный лимитник входа (актуально только если pending_entry_pullback_pct > 0)")
    tp_as_limit_order: bool = Field(default=True, description="Выставлять TP лимитным ордером (maker, 0.02%) вместо market (taker, 0.055%) — цена исполнения та же, экономия на комиссии. SL и time-exit всегда market (надёжность выхода важнее экономии)")


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
