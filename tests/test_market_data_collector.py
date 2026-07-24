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

from datetime import datetime

import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.collectors.market_data import MarketDataCollector
from src.storage.models import Base, Candle

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
