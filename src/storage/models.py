from datetime import datetime

from sqlalchemy import Boolean, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Candle(Base):
    __tablename__ = "candles"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "timestamp", name="uq_candle"),
    )


class Ticker(Base):
    __tablename__ = "tickers"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    last: Mapped[float] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_pct: Mapped[float | None] = mapped_column(Float, nullable=True)


class OpenInterest(Base):
    __tablename__ = "open_interest"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    value: Mapped[float] = mapped_column(Float)


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    setup_type: Mapped[str] = mapped_column(String(64))
    direction: Mapped[str] = mapped_column(String(16))  # long / short
    confidence: Mapped[int] = mapped_column(Integer)
    message: Mapped[str] = mapped_column(Text)
    missed_reason: Mapped[str | None] = mapped_column(String(32), nullable=True, default=None)  # limit / duplicate / cooldown / risk_off / error
    missed_detail: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)  # детали ошибки (исключение, причина) / no_price


class FilteredSignal(Base):
    """Сетапы, отсеянные детектором до появления в signals (после того как объём уже
    подтвердил всплеск) — для анализа, стоит ли ослаблять фильтры. См. AGENTS.md."""

    __tablename__ = "filtered_signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    stage: Mapped[str] = mapped_column(String(32), index=True)  # volume_spike / volume_dump / volume_fading / volume_declining / oi_declining / oi_slope_low / pre_surge_pump / hourly_drop / price_growth_low / exhaustion / exhaustion_extreme / price_growth_high
    reason: Mapped[str] = mapped_column(Text)


class PriceSurgeSignal(Base):
    """Сигналы детектора пампов (strategy_price_surge)."""

    __tablename__ = "price_surge_signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    change_pct: Mapped[float] = mapped_column(Float)
    interval_minutes: Mapped[int] = mapped_column(Integer)


class Trade(Base):
    """Фаза 2: исполненные сделки."""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    signal_id: Mapped[int | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    direction: Mapped[str] = mapped_column(String(16))
    entry_price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[float] = mapped_column(Float)
    entry_time: Mapped[datetime] = mapped_column()
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_time: Mapped[datetime | None] = mapped_column(nullable=True)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="open")  # pending / open / closed / expired / cancelled (cancelled — агент сам отказался от pending-сетапа, в отличие от expired — не докатился по таймауту)
    tp_sl_set: Mapped[bool] = mapped_column(default=False)  # выставлены ли TP/SL на бирже
    partial_closed: Mapped[bool] = mapped_column(default=False)  # выполнено ли частичное закрытие
    partial_pnl: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)  # PnL от частичных закрытий
    fee: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)  # суммарная комиссия по всем "ногам" сделки (pnl уже net-of-fee)
    pending_expires_at: Mapped[datetime | None] = mapped_column(nullable=True, default=None)  # когда снять неисполненный лимитник входа (status=pending)
    source: Mapped[str] = mapped_column(String(16), default="algo", index=True)  # algo / agent — какой пайплайн открыл сделку (разные аккаунты биржи)
    llm_hold_until: Mapped[datetime | None] = mapped_column(nullable=True, default=None)  # ИИ-агент продлил дедлайн max_hold_hours (только увеличивает, не уменьшает)
    llm_hold_extension_total_hours: Mapped[float] = mapped_column(Float, default=0.0)  # накопленное продление, капается agent.max_hold_extension_total_hours
    current_sl_price: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)  # последний известный эффективный стоп (нужен, чтобы агент мог только подтягивать, не ослаблять)
    current_tp_price: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)  # последний известный эффективный тейк (нужен, чтобы агент мог только поднимать, не опускать); None = формульный (entry + SL_distance × risk_reward_ratio)


class AgentDecision(Base):
    """Решение ИИ-агента (доп. режим, отдельный аккаунт) — вход или сопровождение сделки.
    Хранит полный трейс вызовов инструментов для последующего анализа и доработки промпта."""

    __tablename__ = "agent_decisions"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    kind: Mapped[str] = mapped_column(String(16))  # entry / reeval
    signal_id: Mapped[int | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    trade_id: Mapped[int | None] = mapped_column(ForeignKey("trades.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    verdict: Mapped[str] = mapped_column(String(32))  # approve/reject (entry); hold/tighten_sl/extend_hold/close (reeval)
    reasoning: Mapped[str] = mapped_column(Text)
    tool_calls_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON-трейс вызовов инструментов
    applied: Mapped[bool] = mapped_column(Boolean, default=False)  # false в dry_run или если решение не удалось применить
    model: Mapped[str] = mapped_column(String(64))
    agent_version: Mapped[str] = mapped_column(String(16))  # версия системного промпта — для анализа качества решений со временем
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)


class MarketContextSnapshot(Base):
    """Снимок рыночного контекста (BTC/OTHERS/режим/тренд) на момент времени."""

    __tablename__ = "market_context_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    regime: Mapped[str] = mapped_column(String(16))          # risk_on / cautious / risk_off / unknown
    regime_start: Mapped[datetime] = mapped_column()
    trend: Mapped[str] = mapped_column(String(16))            # bullish / bearish / neutral
    trend_start: Mapped[datetime] = mapped_column()
    supertrend_color: Mapped[str] = mapped_column(String(8))  # green / red
    btc_change_1h: Mapped[float] = mapped_column(Float)
    btc_change_4h: Mapped[float] = mapped_column(Float)
    others_value: Mapped[float] = mapped_column(Float)
    others_change_1h: Mapped[float] = mapped_column(Float)
    others_change_4h: Mapped[float] = mapped_column(Float)
    ready: Mapped[bool] = mapped_column(Boolean)
