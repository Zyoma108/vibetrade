from datetime import datetime

from sqlalchemy import Boolean, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Candle(Base):
    """Свечи OHLCV.

    `ix_candles_exchange` удалён 22.09.2026 — ровно тот же случай, что у
    `Ticker` и `OpenInterest`: колонка с двумя различными величинами, по
    которой планировщик может отобрать лишь половину таблицы. Ни один запрос
    в коде не фильтрует по одному `exchange` — везде он идёт в паре с
    `symbol`, а эту пару обслуживает уникальный ключ `uq_candle`. Цена
    индекса была 100 МБ и лишнее обслуживание на каждой вставке.
    """

    __tablename__ = "candles"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32))
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
    """Текущий снимок тикера — ровно одна строка на (exchange, symbol), обновляется
    upsert-ом каждый цикл сбора.

    До 26.08.2026 таблица была append-only и накопила 8 млн строк (1.4 ГБ, половина
    базы) при том, что все три читателя спрашивают только последнее значение.
    Заодно это чинило и патологию планировщика: на миллионах строк SQLite выбирал
    для `_get_current_price` индекс по `exchange` (две различные величины!), отбирал
    по нему половину таблицы и сортировал через TEMP B-TREE — 5.2с на запрос,
    стоящий в пути открытия позиции. Одиночные индексы по exchange/symbol/timestamp
    убраны и возвращать их не нужно: уникальный ключ ниже и есть путь доступа.
    """

    __tablename__ = "tickers"

    __table_args__ = (
        UniqueConstraint("exchange", "symbol", name="uq_ticker"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32))
    timestamp: Mapped[datetime] = mapped_column()  # момент снимка: биржевой, при отсутствии — локальный
    bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    ask: Mapped[float | None] = mapped_column(Float, nullable=True)
    last: Mapped[float] = mapped_column(Float)
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    change_pct: Mapped[float | None] = mapped_column(Float, nullable=True)


class OpenInterest(Base):
    """Журнал открытого интереса — одна строка на изменение значения.

    Составной индекс обязателен. Все горячие читатели (`_write_oi_batch`,
    `DataProvider.load_oi_values`) спрашивают «последние N значений по
    (exchange, symbol)», а одиночных индексов для этого мало: на `exchange`
    всего две различные величины, и планировщик на миллионе строк выбирал
    именно его — отбирал по нему половину таблицы и досортировывал через
    TEMP B-TREE. Замер 01.09.2026 на боевой БД: 13.7 мс на запрос против
    0.17 мс на такой же, но «молодой» базе; в цикле сбора это 8.1 с из 8.5 с
    всего времени записи, и росло линейно с историей. Ровно та же патология,
    что была у `Ticker` (см. её докстринг), — там вылечили, здесь пропустили.

    `ix_open_interest_exchange` удалён как бесполезный (две различные
    величины) — он только удорожал вставку. `ix_open_interest_symbol` удалён
    22.09.2026: он держался на одном запросе
    `PriceSurgeSignalProcessor._calc_oi_change` «по любой бирже», который
    переписан на `exchange.in_(...)` и теперь попадает в составной индекс.
    Цена индекса была несоразмерна: 236 МБ и львиная доля времени записи —
    замер на боевом снапшоте (9.6 млн строк, цикл из 600 монет) дал
    89 мс с тремя индексами против 17 мс без него; INSERT ускорился с 85 до
    10 мс. Индекс по symbol рассеян по 666 значениям, то есть каждая вставка
    — случайная запись страницы.

    `ix_open_interest_timestamp` ОСТАЛСЯ БЕЗ ЧИТАТЕЛЕЙ 23.09.2026: он держался
    на удалении по ретенции, а `retention_days` удалён (вся история нужна для
    бэктестов). Единственный оставшийся запрос к нему — `SELECT MAX(timestamp)`
    в загрузчике бэктеста, и только при `limit_days`: один полный проход на
    загрузку, не в цикле бота. Цена индекса — 13.5% размера БД (20.9 МБ из 155
    на суточной боевой) и доля времени каждой вставки. Кандидат на `DROP INDEX`
    в `init_db()`; не снят, чтобы не смешивать с другой правкой. Подробности:
    `docs/database.md`.
    """

    __tablename__ = "open_interest"

    __table_args__ = (
        Index("ix_oi_exchange_symbol_timestamp", "exchange", "symbol", "timestamp"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32))
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

    # Замер работы по формирующемуся бару (docs/strategy.md, «Формирующийся бар»).
    # Только наблюдение: на вход и на размер позиции не влияет.
    closed_bar_ok: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)  # 1 = сетап есть и на закрытых барах, 0 = нет, NULL = не определено
    closed_bar_stage: Mapped[str | None] = mapped_column(String(32), nullable=True, default=None)  # гейт, валящий сетап на закрытых барах: volume_threshold / volume_* / price_trend / price_growth_low / window_range / exhaustion* / pre_surge_pump / hourly_drop
    last_bar_age_sec: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)  # возраст последнего бара окна на момент сигнала, с (timeframe = 180 → бар закрыт)
    volume_window_shifted: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)  # 1 = объёмное окно взято со сдвигом -1 бар (формирующийся бар отброшен), 0 = как есть. Нужно, чтобы измерить эффект undersized_verdict_min_bar_maturity_pct вживую: бэктестом он непроверяем в принципе

    # Список полей замера, которые обязаны доезжать от детектора до строки БД.
    # Существует потому, что перенос собирался явным списком полей в
    # `core/app.py`, и добавленное 22.09.2026 `volume_window_shifted` в него не
    # попало: датакласс `Signal` без __slots__ молча принял неописанный
    # атрибут, ошибки не было, а колонка стояла NULL у всех сигналов сутки.
    MEASUREMENT_FIELDS = (
        "closed_bar_ok", "closed_bar_stage", "last_bar_age_sec", "volume_window_shifted",
    )

    @classmethod
    def from_detector_signal(cls, sig, timestamp: datetime) -> "Signal":
        """ORM-строка из датакласса `analytics.base.Signal`.

        Единственное место переноса: добавленное в датакласс поле замера
        попадает в БД, только если оно есть в `MEASUREMENT_FIELDS`, а
        отсутствие его в датаклассе роняет тест, а не тихо пишет NULL.
        """
        row = cls(
            timestamp=timestamp,
            symbol=sig.symbol,
            setup_type=sig.setup_type,
            direction=sig.direction,
            confidence=sig.confidence,
            message=sig.message,
        )
        for field in cls.MEASUREMENT_FIELDS:
            value = getattr(sig, field)
            # bool -> int: SQLite хранит INTEGER, а None должен остаться None
            # («вердикт не определён» — не то же самое, что 0).
            setattr(row, field, int(value) if isinstance(value, bool) else value)
        return row


class FilteredSignal(Base):
    """Сетапы, отсеянные детектором до появления в signals (после того как объём уже
    подтвердил всплеск) — для анализа, стоит ли ослаблять фильтры. См. AGENTS.md."""

    __tablename__ = "filtered_signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    stage: Mapped[str] = mapped_column(String(32), index=True)  # volume_spike / volume_dump / volume_fading / volume_declining / oi_declining / oi_slope_low / pre_surge_pump / hourly_drop / price_growth_low / exhaustion / exhaustion_extreme / retracement / price_growth_high
    reason: Mapped[str] = mapped_column(Text)
    # Замер эффекта undersized_verdict_min_bar_maturity_pct (только запись, на
    # решение не влияет). Заполняются лишь для отказов вида «последняя свеча
    # слишком маленькая»: бэктест такой вопрос не воспроизводит в принципе —
    # там нет формирующегося бара, — поэтому цену порога приходится копить на
    # живых данных ДО его включения.
    last_bar_age_sec: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)  # возраст последнего бара окна на момент отказа, с
    shift_would_pass: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)  # 1 = сетап прошёл бы все гейты на окне БЕЗ формирующегося бара


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
    status: Mapped[str] = mapped_column(String(16), default="open")  # pending / open / closed / expired
    tp_sl_set: Mapped[bool] = mapped_column(default=False)  # выставлены ли TP/SL на бирже
    partial_closed: Mapped[bool] = mapped_column(default=False)  # выполнено ли частичное закрытие
    partial_pnl: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)  # PnL от частичных закрытий
    fee: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)  # суммарная комиссия по всем "ногам" сделки (pnl уже net-of-fee)
    pending_expires_at: Mapped[datetime | None] = mapped_column(nullable=True, default=None)  # когда снять неисполненный лимитник входа (status=pending)
    source: Mapped[str] = mapped_column(String(16), default="algo", index=True)  # всегда 'algo'; колонка осталась от удалённого ИИ-режима и скоупит запросы, чтобы его исторические строки не попадали в алго-логику
    current_sl_price: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)  # последний известный эффективный стоп (перевод в безубыток после частичной фиксации)
    signal_price: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)  # референсная цена в момент сигнала — неизменный якорь для замера фактического проскальзывания входа


class BotState(Base):
    """Персистентное состояние Circuit Breaker / бан-листа / error-cooldown
    (`PositionManager`) — до этого фикса жило только в памяти процесса, и
    любой рестарт/деплой бесшумно обнулял защиту от серии убытков и бан-лист
    проблемных монет (см. db-audit-august-2026, P0). Одна строка на source —
    колонка осталась от удалённого ИИ-режима (см. `Trade.source`)."""

    __tablename__ = "bot_state"

    source: Mapped[str] = mapped_column(String(16), primary_key=True)  # всегда 'algo' (см. Trade.source)
    consecutive_losses: Mapped[int] = mapped_column(Integer, default=0)
    circuit_breaker_until: Mapped[datetime | None] = mapped_column(nullable=True, default=None)
    circuit_breaker_stop_consumed_at: Mapped[int] = mapped_column(Integer, default=0)
    banned_symbols_json: Mapped[str] = mapped_column(Text, default="[]")
    error_counts_json: Mapped[str] = mapped_column(Text, default="{}")
    error_cooldown_until_json: Mapped[str] = mapped_column(Text, default="{}")
    updated_at: Mapped[datetime | None] = mapped_column(nullable=True, default=None)


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
