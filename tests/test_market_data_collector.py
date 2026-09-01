"""
Tests for MarketDataCollector candle persistence.

Regression coverage for the "frozen partial-volume candle" bug: the collector
polls the exchange far more often (interval_seconds) than a candle's duration
(timeframe), so the same (exchange, symbol, timestamp) bar is re-fetched many
times while it's still forming. The old code only ever inserted a candle once
and silently skipped every later re-fetch of the same bar — permanently
freezing whatever partial volume/close/high/low happened to exist at the
first poll, even after the bar closed with a much larger real volume on the
exchange. This made the volume-fading/declining filters in SetupDetector
trip on stale data instead of the real, closed-bar volume.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.collectors.market_data import MarketDataCollector
from src.storage.models import Base, Candle, OpenInterest, Ticker

# Naive datetimes: SQLite drops tzinfo on round-trip, so tz-aware timestamps
# written in would come back naive and no longer match dict keys built here.
BASE_TS = datetime(2026, 7, 24, 16, 0, 0)


class FakeConnector:
    """Stub ExchangeConnector: returns whatever OHLCV batch the test hands it."""

    def __init__(self, ohlcv_batches: list[list[dict]]):
        self.exchange_id = "binance"
        self._batches = ohlcv_batches
        self._call = 0

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "3m", limit: int = 100):
        batch = self._batches[min(self._call, len(self._batches) - 1)]
        self._call += 1
        return batch

    async def fetch_open_interest(self, symbol: str):
        return None


def _bar(volume: float, timestamp: datetime = BASE_TS, close: float = 1.0) -> dict:
    return {
        "exchange": "binance",
        "symbol": "TEST/USDT",
        "timestamp": timestamp,
        "open": 1.0,
        "high": close,
        "low": 1.0,
        "close": close,
        "volume": volume,
    }


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


def _make_collector() -> MarketDataCollector:
    return MarketDataCollector(
        connectors=[], exclude_coins=[], min_volume_usdt=0.0,
        interval_seconds=15, timeframe="3m",
    )


def _selected_ticker() -> list[dict]:
    return [{
        "exchange": "binance",
        "symbol": "TEST/USDT",
        "timestamp": BASE_TS,
        "last": 1.0,
    }]


async def test_repeated_poll_updates_still_forming_candle(session_factory):
    """A bar re-fetched mid-formation must have its volume/close updated, not frozen."""
    collector = _make_collector()
    connector = FakeConnector([
        [_bar(volume=5_000.0, close=1.0)],   # poll right after the bar opens
        [_bar(volume=40_000.0, close=1.2)],  # poll mid-bar: volume grew a lot
        [_bar(volume=90_000.0, close=1.5)],  # poll at close: final real volume
    ])
    selected = _selected_ticker()

    async with session_factory() as session:
        for _ in range(3):
            await collector._collect_for_exchange(connector, session, selected)

        rows = (await session.execute(select(Candle))).scalars().all()

    assert len(rows) == 1, "must not duplicate the row for the same bar"
    assert rows[0].volume == 90_000.0, (
        f"volume should reflect the final closed-bar value, got {rows[0].volume} "
        "(stuck on an early partial poll = the regression this guards against)"
    )
    assert rows[0].close == 1.5


async def test_closed_older_bar_is_not_rewritten_by_stale_refetch(session_factory):
    """Older bars the exchange still returns unchanged must not lose new data to no-ops."""
    collector = _make_collector()
    older_ts = BASE_TS
    newer_ts = datetime(2026, 7, 24, 16, 3, 0)

    connector = FakeConnector([
        [_bar(volume=10_000.0, timestamp=older_ts), _bar(volume=2_000.0, timestamp=newer_ts)],
        [_bar(volume=10_000.0, timestamp=older_ts), _bar(volume=30_000.0, timestamp=newer_ts)],
    ])
    selected = _selected_ticker()

    async with session_factory() as session:
        await collector._collect_for_exchange(connector, session, selected)
        await collector._collect_for_exchange(connector, session, selected)

        rows = {
            r.timestamp: r
            for r in (await session.execute(select(Candle))).scalars().all()
        }

    assert len(rows) == 2
    assert rows[older_ts].volume == 10_000.0  # unchanged bar stays unchanged
    assert rows[newer_ts].volume == 30_000.0  # still-forming bar picked up the growth


async def test_new_bar_is_inserted_once(session_factory):
    """A brand-new bar not seen before is inserted (not skipped)."""
    collector = _make_collector()
    connector = FakeConnector([[_bar(volume=1_234.0)]])
    selected = _selected_ticker()

    async with session_factory() as session:
        await collector._collect_for_exchange(connector, session, selected)
        rows = (await session.execute(select(Candle))).scalars().all()

    assert len(rows) == 1
    assert rows[0].volume == 1_234.0


async def test_tz_aware_candles_from_real_connector_do_not_crash(session_factory):
    """Regression: ExchangeConnector.fetch_ohlcv() returns tz-aware (UTC) timestamps,
    but SQLite drops tzinfo on round-trip (see module docstring), so max(Candle.timestamp)
    read back from the DB is naive. Comparing the two directly used to raise
    `TypeError: can't compare offset-naive and offset-aware datetimes`."""
    collector = _make_collector()
    aware_ts = BASE_TS.replace(tzinfo=timezone.utc)
    newer_aware_ts = datetime(2026, 7, 24, 16, 3, 0, tzinfo=timezone.utc)
    connector = FakeConnector([
        [_bar(volume=5_000.0, timestamp=aware_ts)],
        [_bar(volume=5_000.0, timestamp=aware_ts), _bar(volume=1_000.0, timestamp=newer_aware_ts)],
    ])
    selected = _selected_ticker()

    async with session_factory() as session:
        await collector._collect_for_exchange(connector, session, selected)
        await collector._collect_for_exchange(connector, session, selected)
        rows = (await session.execute(select(Candle))).scalars().all()

    assert len(rows) == 2


# ---------------------------------------------------------------------------
# tickers как снимок, а не журнал (26.08.2026)
# ---------------------------------------------------------------------------


def _ticker(symbol: str, exchange: str, last: float, ts: datetime = BASE_TS) -> dict:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "timestamp": ts,
        "bid": last - 0.5,
        "ask": last + 0.5,
        "last": last,
        "volume": 1_000.0,
        "change_pct": 2.5,
        "open_interest": 777.0,  # приезжает в тикере ByBit, в модель попасть не должен
    }


async def test_upsert_tickers_keeps_one_row_per_coin(session_factory):
    """Каждый цикл переписывает ту же строку, а не добавляет новую.

    До этой смены таблица была append-only и набрала 8 млн строк (половина БД)
    при том, что читается только последнее значение.
    """
    collector = _make_collector()
    async with session_factory() as session:
        for i in range(5):
            await collector._upsert_tickers(session, [
                _ticker("AAA/USDT:USDT", "bybit", 100.0 + i),
                _ticker("BBB/USDT:USDT", "binance", 200.0 + i),
            ])
            await session.commit()

        rows = (await session.execute(select(Ticker).order_by(Ticker.symbol))).scalars().all()

    assert len(rows) == 2, "пять циклов по двум монетам должны дать две строки"
    assert rows[0].last == 104.0, "должно остаться последнее значение"
    assert rows[1].last == 204.0


async def test_upsert_tickers_separates_exchanges(session_factory):
    """Одна монета на двух биржах — две независимые строки, а не перезапись."""
    collector = _make_collector()
    async with session_factory() as session:
        await collector._upsert_tickers(session, [
            _ticker("AAA/USDT:USDT", "bybit", 100.0),
            _ticker("AAA/USDT:USDT", "binance", 101.0),
        ])
        await session.commit()
        rows = (await session.execute(select(Ticker).order_by(Ticker.exchange))).scalars().all()

    assert len(rows) == 2
    assert {r.exchange: r.last for r in rows} == {"binance": 101.0, "bybit": 100.0}


async def test_upsert_tickers_drops_open_interest_key(session_factory):
    """`open_interest` едет в тикере ByBit, но колонки под него нет — не должен ломать вставку."""
    collector = _make_collector()
    async with session_factory() as session:
        await collector._upsert_tickers(session, [_ticker("AAA/USDT:USDT", "bybit", 100.0)])
        await session.commit()
        row = (await session.execute(select(Ticker))).scalar_one()

    assert row.last == 100.0
    assert not hasattr(row, "open_interest")


async def test_upsert_tickers_empty_batch_is_noop(session_factory):
    """Пустой список — не падать на INSERT без VALUES."""
    collector = _make_collector()
    async with session_factory() as session:
        await collector._upsert_tickers(session, [])
        await session.commit()
        assert (await session.execute(select(Ticker))).scalars().all() == []


# ---------------------------------------------------------------------------
# История для бэктестов: свечи и OI пишутся по ВСЕМ сканируемым монетам
# ---------------------------------------------------------------------------


class _MultiCoinConnector:
    """Заглушка биржи на несколько монет — ни одна из них не «торгуется»."""

    def __init__(self, exchange_id: str, symbols: list[str], volume: float = 5_000_000.0):
        self.exchange_id = exchange_id
        self._symbols = symbols
        self._volume = volume
        self._poll = 0

    async def fetch_tickers(self) -> list[dict]:
        self._poll += 1
        return [
            {
                "exchange": self.exchange_id, "symbol": s, "timestamp": BASE_TS,
                "bid": 1.0, "ask": 1.1, "last": 1.0 + self._poll * 0.01,
                "volume": self._volume, "change_pct": 1.0,
                # OI меняется от цикла к циклу — иначе _write_oi дедуплицирует его
                "open_interest": 1000.0 + self._poll,
            }
            for s in self._symbols
        ]

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "3m", limit: int = 100):
        # На каждом опросе — новый закрытый бар, чтобы копилась история
        return [
            {
                "exchange": self.exchange_id, "symbol": symbol,
                "timestamp": BASE_TS + timedelta(minutes=3 * i),
                "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0,
                "volume": 100.0 * (i + 1),
            }
            for i in range(self._poll)
        ]

    async def fetch_open_interest(self, symbol: str):
        return {
            "exchange": self.exchange_id, "symbol": symbol,
            "timestamp": BASE_TS, "value": 1000.0 + self._poll,
        }


async def test_history_kept_for_all_scanned_coins(session_factory, monkeypatch):
    """Свечи и OI копятся по КАЖДОЙ сканируемой монете, даже если ею не торгуют.

    Это фундамент бэктестов: движок читает candles/open_interest, а не tickers.
    Перевод tickers на снимок (одна строка на монету) историю рынка не трогает —
    зафиксировано здесь, чтобы это нельзя было сломать незаметно.
    """
    import src.collectors.market_data as md

    monkeypatch.setattr(md, "async_session", session_factory)
    symbols = ["AAA/USDT:USDT", "BBB/USDT:USDT", "CCC/USDT:USDT"]
    collector = MarketDataCollector(
        connectors=[_MultiCoinConnector("bybit", symbols)],
        exclude_coins=[], min_volume_usdt=0.0, interval_seconds=15, timeframe="3m",
    )

    for _ in range(3):  # три цикла сбора подряд
        await collector._collect_cycle()

    async with session_factory() as session:
        candles = (await session.execute(select(Candle))).scalars().all()
        ois = (await session.execute(select(OpenInterest))).scalars().all()
        tickers = (await session.execute(select(Ticker))).scalars().all()

    # Свечи: история по каждой монете, а не только последний бар
    by_symbol: dict[str, set] = {}
    for c in candles:
        by_symbol.setdefault(c.symbol, set()).add(c.timestamp)
    assert set(by_symbol) == set(symbols), "свечи должны быть по всем монетам"
    for sym in symbols:
        assert len(by_symbol[sym]) == 3, f"{sym}: ожидалось 3 бара истории"

    # OI: тоже история (дедуп только по неизменившемуся значению)
    oi_by_symbol: dict[str, int] = {}
    for oi in ois:
        oi_by_symbol[oi.symbol] = oi_by_symbol.get(oi.symbol, 0) + 1
    assert set(oi_by_symbol) == set(symbols)
    assert all(count == 3 for count in oi_by_symbol.values()), oi_by_symbol

    # Тикеры: ровно один снимок на монету — это и есть экономия места
    assert len(tickers) == len(symbols)


# ---------------------------------------------------------------------------
# Цена записи: коммитов на цикл должно быть на два порядка меньше числа монет
# ---------------------------------------------------------------------------


class _ManyCoinConnector:
    """Заглушка на произвольное число монет: один новый бар и свой OI на монету."""

    def __init__(self, symbols: list[str], oi_offset: float = 0.0):
        self.exchange_id = "binance"
        self._symbols = symbols
        self._oi_offset = oi_offset

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "3m", limit: int = 100):
        return [{
            "exchange": "binance", "symbol": symbol, "timestamp": BASE_TS,
            "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100.0,
        }]

    async def fetch_open_interest(self, symbol: str):
        return {
            "exchange": "binance", "symbol": symbol, "timestamp": BASE_TS,
            "value": 1000.0 + self._oi_offset + len(symbol),
        }


def _selected(symbols: list[str]) -> list[dict]:
    return [{
        "exchange": "binance", "symbol": s, "timestamp": BASE_TS, "last": 1.0,
    } for s in symbols]


async def test_write_path_commits_are_batched(session_factory):
    """Регресс: раньше здесь был коммит НА КАЖДУЮ монету — по одному в
    `_upsert_candles` и по одному на каждый изменившийся OI, около 1050 fsync'ов
    за цикл. С честным барьером записи (Linux) это ~22 с на ровном месте.

    Тест сторожит не скорость (её в юнит-тесте не померить), а само число
    коммитов: оно должно расти как N/COMMIT_CHUNK, а не как N.
    """
    from src.collectors.market_data import COMMIT_CHUNK

    n = COMMIT_CHUNK * 3
    symbols = [f"C{i:04d}/USDT:USDT" for i in range(n)]
    collector = _make_collector()
    connector = _ManyCoinConnector(symbols)

    commits = 0
    async with session_factory() as session:
        original = session.commit

        async def counting_commit():
            nonlocal commits
            commits += 1
            await original()

        session.commit = counting_commit
        await collector._collect_for_exchange(connector, session, _selected(symbols))

        rows = (await session.execute(select(Candle))).scalars().all()
        ois = (await session.execute(select(OpenInterest))).scalars().all()

    assert len(rows) == n, "все свечи должны быть записаны"
    assert len(ois) == n, "OI должен быть записан по каждой монете"
    # 3 чанка свечей + финальный коммит свечей + один коммит фазы OI + запас
    assert commits <= n // COMMIT_CHUNK + 4, (
        f"коммитов {commits} на {n} монет — похоже, вернулся коммит на монету"
    )
    assert commits < n / 10, f"коммитов {commits}, ожидалось на порядок меньше {n}"


async def test_oi_unchanged_value_is_not_rewritten(session_factory):
    """Дедупликация OI переживает переход на батчевую запись: одинаковое
    значение подряд не должно плодить строки."""
    symbols = ["AAA/USDT:USDT", "BBB/USDT:USDT"]
    collector = _make_collector()
    connector = _ManyCoinConnector(symbols)

    async with session_factory() as session:
        await collector._collect_for_exchange(connector, session, _selected(symbols))
        await collector._collect_for_exchange(connector, session, _selected(symbols))
        first = (await session.execute(select(OpenInterest))).scalars().all()

        # то же самое, но OI изменился — теперь строки добавиться должны
        await collector._collect_for_exchange(
            connector, session, _selected(symbols),
        )
        changed_connector = _ManyCoinConnector(symbols, oi_offset=5.0)
        await collector._collect_for_exchange(
            changed_connector, session, _selected(symbols),
        )
        second = (await session.execute(select(OpenInterest))).scalars().all()

    assert len(first) == len(symbols), "повтор того же OI не должен плодить строки"
    assert len(second) == len(symbols) * 2, "изменившийся OI должен быть записан"


async def test_fetch_phase_has_a_hard_deadline(monkeypatch):
    """Верхняя граница на фазу фетча — то, что делает каданс скана гарантией.

    Дедлайн на отдельный вызов снижает шанс зависнуть, но 580 вызовов по 8 с
    в худшем случае всё равно дают неприемлемо длинный цикл. Не успевшие
    монеты должны быть отменены, а успевшие — возвращены.
    """
    import src.collectors.market_data as md

    monkeypatch.setattr(md, "SCAN_PHASE_TIMEOUT_SEC", 0.3)
    collector = _make_collector()

    class _Conn:
        exchange_id = "binance"

    async def quick(n):
        return ("быстрая", n)

    async def slow(n):
        await asyncio.sleep(5)
        return ("медленная", n)

    t = time.perf_counter()
    got = await collector._gather_with_deadline(
        _Conn(), [quick(1), quick(2), slow(3), slow(4)], "свечи",
    )
    elapsed = time.perf_counter() - t

    assert elapsed < 2.0, f"фаза шла {elapsed:.2f}с при дедлайне 0.3с"
    assert sorted(got) == [("быстрая", 1), ("быстрая", 2)], (
        f"успевшие монеты должны вернуться, зависшие — отмениться, получено {got}"
    )


# ---------------------------------------------------------------------------
# Ретенция: история не должна расти без верхней границы
# ---------------------------------------------------------------------------


async def test_retention_drops_old_rows_and_keeps_recent(session_factory, monkeypatch):
    """Регресс: удаления не было вообще — за 5 суток БД набирала 459 МБ,
    архивная за 15 суток весит 3.2 ГБ. Каждая лишняя строка удорожает вставку
    (правятся B-деревья индексов), то есть история платит за себя каждый цикл.
    """
    import src.collectors.market_data as md

    monkeypatch.setattr(md, "async_session", session_factory)
    monkeypatch.setattr(md, "CLEANUP_BATCH", 3)  # чистка обязана идти порциями

    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    old = now - timedelta(days=40)
    recent = now - timedelta(days=2)

    async with session_factory() as session:
        for i in range(10):
            session.add(Candle(
                exchange="binance", symbol=f"S{i}/USDT", timestamp=old,
                open=1, high=1, low=1, close=1, volume=1,
            ))
            session.add(OpenInterest(
                exchange="binance", symbol=f"S{i}/USDT", timestamp=old, value=1.0,
            ))
        for i in range(4):
            session.add(Candle(
                exchange="binance", symbol=f"N{i}/USDT", timestamp=recent,
                open=1, high=1, low=1, close=1, volume=1,
            ))
        await session.commit()

    collector = MarketDataCollector(
        connectors=[], exclude_coins=[], min_volume_usdt=0.0,
        interval_seconds=15, timeframe="3m", retention_days=30,
    )
    await collector._cleanup_old_data()

    async with session_factory() as session:
        candles = (await session.execute(select(Candle))).scalars().all()
        ois = (await session.execute(select(OpenInterest))).scalars().all()

    assert len(candles) == 4, "старые свечи должны быть удалены, свежие — остаться"
    assert all(c.timestamp == recent for c in candles)
    assert ois == [], "старый OI должен быть удалён"


async def test_retention_runs_at_most_once_a_day(session_factory, monkeypatch):
    """Чистка не должна идти каждый цикл: цикл — это полторы минуты."""
    import src.collectors.market_data as md

    monkeypatch.setattr(md, "async_session", session_factory)
    collector = MarketDataCollector(
        connectors=[], exclude_coins=[], min_volume_usdt=0.0,
        interval_seconds=15, timeframe="3m", retention_days=30,
    )

    await collector._cleanup_old_data()
    first = collector._last_cleanup
    assert first is not None

    await collector._cleanup_old_data()
    assert collector._last_cleanup == first, "второй вызов подряд должен быть no-op"


async def test_retention_disabled_by_zero(session_factory, monkeypatch):
    import src.collectors.market_data as md

    monkeypatch.setattr(md, "async_session", session_factory)
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    async with session_factory() as session:
        session.add(Candle(
            exchange="binance", symbol="OLD/USDT", timestamp=now - timedelta(days=999),
            open=1, high=1, low=1, close=1, volume=1,
        ))
        await session.commit()

    collector = MarketDataCollector(
        connectors=[], exclude_coins=[], min_volume_usdt=0.0,
        interval_seconds=15, timeframe="3m", retention_days=0,
    )
    await collector._cleanup_old_data()

    async with session_factory() as session:
        assert len((await session.execute(select(Candle))).scalars().all()) == 1
