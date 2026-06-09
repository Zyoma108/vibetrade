"""
Tests for CandleCache and DataProvider.

Verifies candles are not lost across refresh cycles at intervals
simulating 5, 10, and 15 minute collector cycles.
"""

from datetime import datetime, timedelta

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.data_provider import CandleCache, DataProvider
from src.storage.models import Base, Candle

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def engine():
    """In-memory SQLite engine with the candles table."""
    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    """Return an async_sessionmaker bound to the in-memory engine."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def session(session_factory):
    """A single session."""
    async with session_factory() as sess:
        yield sess


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TIME = datetime(2026, 6, 1, 0, 0, 0)


def _make_candle(
    exchange: str = "binance",
    symbol: str = "TEST/USDT",
    minutes_offset: int = 0,
    volume: float = 100_000.0,
    price: float = 1.0,
) -> Candle:
    """Create a Candle ORM object at BASE_TIME + minutes_offset."""
    ts = BASE_TIME + timedelta(minutes=minutes_offset)
    jitter = price * 0.001
    return Candle(
        exchange=exchange,
        symbol=symbol,
        timestamp=ts,
        open=price,
        high=price + jitter,
        low=price - jitter,
        close=price,
        volume=volume,
    )


async def _seed_candles(session, exchange: str, symbol: str, count: int):
    """Insert ``count`` candles for a symbol, spaced 3 minutes apart, oldest first."""
    candles = [
        _make_candle(exchange, symbol, minutes_offset=i * 3, volume=100_000.0 + i * 1000)
        for i in range(count)
    ]
    session.add_all(candles)
    await session.commit()


async def _add_new_candles(session, exchange: str, symbol: str, count: int, start_offset: int):
    """Add ``count`` more candles *after* the last seed candle."""
    candles = [
        _make_candle(exchange, symbol, minutes_offset=start_offset + i * 3,
                      volume=200_000.0 + i * 500)
        for i in range(count)
    ]
    session.add_all(candles)
    await session.commit()


def _candle_timestamps(candles: list[dict]) -> list:
    """Extract timestamps from cached candle dicts."""
    return [c["timestamp"] for c in candles]


def _candle_volumes(candles: list[dict]) -> list:
    return [c["volume"] for c in candles]


# ---------------------------------------------------------------------------
# CandleCache tests
# ---------------------------------------------------------------------------


class TestCandleCacheInitialLoad:
    """First load for a symbol — full history fetch."""

    async def test_first_load_exact_limit(self, session_factory):
        """First load returns exactly ``limit`` candles."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "TEST/USDT", count=100)

        cache = CandleCache()
        async with session_factory() as sess:
            candles = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=84)

        assert len(candles) == 84, f"Expected 84, got {len(candles)}"
        # Should be the 84 most recent (last 84 of 100)
        assert len(cache._candles["binance:TEST/USDT"]) == 84

    async def test_first_load_less_than_limit(self, session_factory):
        """When DB has fewer candles than limit, return what exists."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "SMALL/USDT", count=20)

        cache = CandleCache()
        async with session_factory() as sess:
            candles = await cache.load_or_refresh(sess, "binance", "SMALL/USDT", limit=84)

        assert len(candles) == 20
        assert cache._max_ts["binance:SMALL/USDT"] is not None

    async def test_symbols_are_independent(self, session_factory):
        """Two symbols share nothing in the cache."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "A/USDT", count=50)
            await _seed_candles(sess, "binance", "B/USDT", count=30)

        cache = CandleCache()
        async with session_factory() as sess:
            a = await cache.load_or_refresh(sess, "binance", "A/USDT", limit=84)
            b = await cache.load_or_refresh(sess, "binance", "B/USDT", limit=84)

        assert len(a) == 50
        assert len(b) == 30
        assert cache._max_ts["binance:A/USDT"] != cache._max_ts["binance:B/USDT"]


class TestCandleCacheRefresh:
    """Subsequent cycles — only delta is fetched."""

    async def test_refresh_after_5_minute_cycle(self, session_factory):
        """Simulate a 5-minute collector cycle: 1-2 new candles added."""
        limit = 84

        # Seed initial candles
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "TEST/USDT", count=limit)

        cache = CandleCache()

        # First cycle — full load
        async with session_factory() as sess:
            first = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        assert len(first) == limit
        first_last_ts = cache._max_ts["binance:TEST/USDT"]

        # Simulate 5 minutes passing: 1 new candle (3m tf → 5min could add 1-2)
        async with session_factory() as sess:
            # The last candle was at BASE_TIME + (limit-1)*3 min
            # Add candles after that
            await _add_new_candles(sess, "binance", "TEST/USDT", count=2,
                                   start_offset=(limit - 1) * 3 + 3)

        # Second cycle — should only fetch 2 new candles
        async with session_factory() as sess:
            second = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        assert len(second) == limit, f"Still {limit} candles returned, got {len(second)}"
        # The latest timestamp should have advanced
        new_last_ts = cache._max_ts["binance:TEST/USDT"]
        assert new_last_ts > first_last_ts
        # First candle of "first" should no longer be in "second"
        # (slide by 2: oldest 2 dropped, 2 new added)
        assert _candle_timestamps(first)[-1] < _candle_timestamps(second)[-1]

    async def test_refresh_after_10_minute_cycle(self, session_factory):
        """Simulate a 10-minute collector cycle: 3-4 new candles (3m tf)."""
        limit = 84

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "TEST/USDT", count=limit)

        cache = CandleCache()

        async with session_factory() as sess:
            first = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        # 10 minutes = ~3 new candles on 3m timeframe
        async with session_factory() as sess:
            await _add_new_candles(sess, "binance", "TEST/USDT", count=3,
                                   start_offset=(limit - 1) * 3 + 3)

        async with session_factory() as sess:
            second = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        assert len(second) == limit
        # Verify the 3 newest candles in second are the ones we added
        newest_volumes = _candle_volumes(second)[-3:]
        expected = [200_000.0 + i * 500 for i in range(3)]
        assert newest_volumes == expected, f"Expected {expected}, got {newest_volumes}"

    async def test_refresh_after_15_minute_cycle(self, session_factory):
        """Simulate a 15-minute collector cycle: 5 new candles (3m tf)."""
        limit = 84
        add_count = 5

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "TEST/USDT", count=limit)

        cache = CandleCache()

        async with session_factory() as sess:
            first = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        async with session_factory() as sess:
            await _add_new_candles(sess, "binance", "TEST/USDT", count=add_count,
                                   start_offset=(limit - 1) * 3 + 3)

        async with session_factory() as sess:
            second = await cache.load_or_refresh(sess, "binance", "TEST/USDT", limit=limit)

        assert len(second) == limit
        # Oldest 5 should have been pushed out
        assert _candle_volumes(first)[:add_count] != _candle_volumes(second)[:add_count]
        # Newest 5 should be the added ones
        newest = _candle_volumes(second)[-add_count:]
        expected = [200_000.0 + i * 500 for i in range(add_count)]
        assert newest == expected, f"Expected {expected}, got {newest}"

    async def test_refresh_with_no_new_candles(self, session_factory):
        """When no new candles in DB, cache returns same data unchanged."""
        limit = 84

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "NOCHANGE/USDT", count=limit)

        cache = CandleCache()

        async with session_factory() as sess:
            first = await cache.load_or_refresh(sess, "binance", "NOCHANGE/USDT", limit=limit)

        # No new candles added — simulate same cycle
        async with session_factory() as sess:
            second = await cache.load_or_refresh(sess, "binance", "NOCHANGE/USDT", limit=limit)

        assert len(second) == limit
        assert len(cache._candles["binance:NOCHANGE/USDT"]) == limit
        assert _candle_timestamps(first) == _candle_timestamps(second)
        # max_ts unchanged
        assert cache._max_ts["binance:NOCHANGE/USDT"] == _candle_timestamps(first)[-1]

    async def test_multiple_cycles_no_data_loss(self, session_factory):
        """Over 10 cycles, candles slide correctly — no gaps, no data loss."""
        limit = 84
        cycles = 10
        add_per_cycle = 2  # ~6 min cycles on 3m tf

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "SLIDE/USDT", count=limit)

        cache = CandleCache()
        last_timeline: list[datetime] = []

        for cycle in range(cycles):
            async with session_factory() as sess:
                candles = await cache.load_or_refresh(sess, "binance", "SLIDE/USDT", limit=limit)

            assert len(candles) == limit, f"Cycle {cycle}: got {len(candles)} candles"

            current_timeline = _candle_timestamps(candles)
            if last_timeline:
                # New timeline should be shifted forward by add_per_cycle
                # The overlap should be limit - add_per_cycle candles
                overlap_start = current_timeline[0]
                # Find where the old timeline had this timestamp
                if overlap_start in last_timeline:
                    idx = last_timeline.index(overlap_start)
                    assert idx == add_per_cycle, (
                        f"Cycle {cycle}: expected overlap offset {add_per_cycle}, "
                        f"got {idx}"
                    )

            last_timeline = current_timeline

            # Add new candles for next cycle
            base_idx = (limit - 1) * 3 + 3 + cycle * add_per_cycle * 3
            async with session_factory() as sess:
                await _add_new_candles(sess, "binance", "SLIDE/USDT",
                                       count=add_per_cycle,
                                       start_offset=base_idx)


class TestCandleCacheDifferentLimits:
    """SetupDetector (84) vs PriceSurgeDetector (11) — cache handles both."""

    async def test_large_then_small_limit(self, session_factory):
        """First load with 84, then request with 11 — served from cache, no DB hit."""
        limit_big = 84
        limit_small = 11

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "MULTI/USDT", count=100)

        cache = CandleCache()

        async with session_factory() as sess:
            big = await cache.load_or_refresh(sess, "binance", "MULTI/USDT", limit=limit_big)

        # Small limit request — should serve from cache (≥ limit_small, so no re-fetch)
        async with session_factory() as sess:
            small = await cache.load_or_refresh(sess, "binance", "MULTI/USDT", limit=limit_small)

        assert len(small) == limit_small
        # The small set should be the tail of the big set
        assert _candle_timestamps(small) == _candle_timestamps(big)[-limit_small:]

    async def test_small_then_large_limit(self, session_factory):
        """First load with 11, then request with 84 — triggers full reload."""
        limit_small = 11
        limit_big = 84

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "MULTI2/USDT", count=100)

        cache = CandleCache()

        async with session_factory() as sess:
            small = await cache.load_or_refresh(sess, "binance", "MULTI2/USDT", limit=limit_small)

        # Big limit request — cache has only 11, which is < 84 → full reload
        async with session_factory() as sess:
            big = await cache.load_or_refresh(sess, "binance", "MULTI2/USDT", limit=limit_big)

        assert len(big) == limit_big
        # Cache now stores 84, not 11
        assert len(cache._candles["binance:MULTI2/USDT"]) == limit_big


class TestCandleCacheEdgeCases:
    """Edge cases and error handling."""

    async def test_new_symbol_mid_run(self, session_factory):
        """Symbol appears in DB after cache was already in use."""
        # Pre-populate cache with one symbol
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "OLD/USDT", count=50)

        cache = CandleCache()
        async with session_factory() as sess:
            await cache.load_or_refresh(sess, "binance", "OLD/USDT", limit=84)

        # Later, a new symbol gets candles
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "NEW/USDT", count=30)

        async with session_factory() as sess:
            new_candles = await cache.load_or_refresh(sess, "binance", "NEW/USDT", limit=84)

        assert len(new_candles) == 30
        assert "binance:NEW/USDT" in cache._candles
        assert "binance:OLD/USDT" in cache._candles  # Old symbol still cached

    async def test_zero_volume_candles_skipped(self, session_factory):
        """Candles with volume=0 (unclosed) are excluded.

        _fetch_recent uses LIMIT 84.  With 87 normals + 3 zeros at the end,
        the 84 most recent include 81 normals + 3 zeros.  After filtering
        the zeros we get exactly 81 — proving the 3 zero-volume entries
        were excluded.
        """
        limit = 84
        async with session_factory() as sess:
            for i in range(limit + 3):
                sess.add(_make_candle("binance", "ZV/USDT", minutes_offset=i * 3, volume=100_000.0))
            # Zero-volume candles (unclosed) at the very end
            base = limit + 3
            for i in range(3):
                sess.add(_make_candle("binance", "ZV/USDT", minutes_offset=(base + i) * 3, volume=0.0))
            await sess.commit()

        cache = CandleCache()
        async with session_factory() as sess:
            candles = await cache.load_or_refresh(sess, "binance", "ZV/USDT", limit=limit)

        # 84 fetched, 3 zero-vol excluded → 81 returned
        assert len(candles) == limit - 3
        assert all(c["volume"] > 0 for c in candles)

    async def test_empty_db_returns_empty(self, session_factory):
        """Symbol with no candles returns empty list."""
        cache = CandleCache()
        async with session_factory() as sess:
            candles = await cache.load_or_refresh(sess, "binance", "GHOST/USDT", limit=84)

        assert candles == []
        assert cache._candles["binance:GHOST/USDT"] == []

    async def test_many_cycles_stable_memory(self, session_factory):
        """After many cycles, cache size per symbol remains bounded to limit."""
        limit = 84
        symbols = 5
        cycles = 20

        for sym_idx in range(symbols):
            async with session_factory() as sess:
                await _seed_candles(sess, "binance", f"S{sym_idx}/USDT", count=limit)

        cache = CandleCache()

        for cycle in range(cycles):
            for sym_idx in range(symbols):
                sym = f"S{sym_idx}/USDT"
                async with session_factory() as sess:
                    candles = await cache.load_or_refresh(sess, "binance", sym, limit=limit)
                assert len(candles) == limit

            # Add 2 new candles per symbol per cycle
            for sym_idx in range(symbols):
                base_idx = (limit - 1) * 3 + 3 + cycle * 2 * 3
                async with session_factory() as sess:
                    await _add_new_candles(sess, "binance", f"S{sym_idx}/USDT",
                                           count=2, start_offset=base_idx)

        # All caches bounded to limit
        for sym_idx in range(symbols):
            key = f"binance:S{sym_idx}/USDT"
            assert len(cache._candles[key]) == limit, f"{key}: {len(cache._candles[key])} > {limit}"


# ---------------------------------------------------------------------------
# DataProvider tests (integration with intra-cycle cache)
# ---------------------------------------------------------------------------


class TestDataProviderWithPersistentCache:
    """DataProvider correctly delegates to CandleCache and populates intra-cycle cache."""

    async def test_first_call_uses_persistent_cache(self, session_factory):
        """First load_candles call goes through CandleCache, populates intra-cycle."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "DP/USDT", count=100)

        pcache = CandleCache()
        dp = DataProvider(candle_cache=pcache)

        async with session_factory() as sess:
            result = await dp.load_candles(sess, "binance", "DP/USDT", limit=84)

        assert len(result) == 84
        # Intra-cycle cache populated
        assert len(dp._candles["binance:DP/USDT"]) == 84
        # Result dicts do NOT have 'timestamp' (stripped for detector compatibility)
        assert "timestamp" not in result[0]
        assert "open" in result[0]

    async def test_second_detector_hits_intra_cycle_cache(self, session_factory):
        """Within same cycle, second detector gets data from intra-cycle cache — no DB call."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "INTRA/USDT", count=100)

        pcache = CandleCache()
        dp = DataProvider(candle_cache=pcache)

        async with session_factory() as sess:
            # First "detector" call
            first = await dp.load_candles(sess, "binance", "INTRA/USDT", limit=84)

            # Modify intra-cycle to verify second call hits it
            dp._candles["binance:INTRA/USDT"] = [{"open": 9.9, "high": 9.9, "low": 9.9, "close": 9.9, "volume": 9.9}]

            # Second "detector" call with small limit
            second = await dp.load_candles(sess, "binance", "INTRA/USDT", limit=1)

        # Second call returned our injected marker, proving it hit intra-cycle cache
        assert second[0]["open"] == 9.9

    async def test_without_persistent_cache_falls_back_to_db(self, session_factory):
        """When no CandleCache provided, DataProvider works as before (DB direct)."""
        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "NOCACHE/USDT", count=100)

        dp = DataProvider()  # No cache

        async with session_factory() as sess:
            result = await dp.load_candles(sess, "binance", "NOCACHE/USDT", limit=84)

        assert len(result) == 84

    async def test_cycle_boundary_new_candles_appear(self, session_factory):
        """Between DataProvider instances (cycles), new candles are picked up."""
        limit = 84

        async with session_factory() as sess:
            await _seed_candles(sess, "binance", "CYCLE/USDT", count=limit)

        pcache = CandleCache()

        # Cycle 1
        dp1 = DataProvider(candle_cache=pcache)
        async with session_factory() as sess:
            r1 = await dp1.load_candles(sess, "binance", "CYCLE/USDT", limit=limit)

        # Add new candles between cycles
        async with session_factory() as sess:
            await _add_new_candles(sess, "binance", "CYCLE/USDT", count=2,
                                   start_offset=(limit - 1) * 3 + 3)

        # Cycle 2 — new DataProvider, same persistent cache
        dp2 = DataProvider(candle_cache=pcache)
        async with session_factory() as sess:
            r2 = await dp2.load_candles(sess, "binance", "CYCLE/USDT", limit=limit)

        assert len(r2) == limit
        # The newest candle advanced
        assert r1[-1]["volume"] != r2[-1]["volume"]
        # r2's oldest candle is r1's second candle (slid by 2)
        # Compare volumes — they should match after a 2-candle slide
        assert r1[2]["volume"] == r2[0]["volume"]

    async def test_different_refresh_intervals(self, session_factory):
        """Parameterized: 5, 10, 15 minute cycles preserve candle integrity."""
        limit = 84

        for delay_minutes, expected_new in [(5, 2), (10, 4), (15, 5)]:
            # Fresh DB and cache per interval
            eng = create_async_engine("sqlite+aiosqlite://", echo=False)
            async with eng.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            sf = async_sessionmaker(eng, class_=AsyncSession, expire_on_commit=False)

            async with sf() as sess:
                await _seed_candles(sess, "binance", f"DELAY{delay_minutes}/USDT", count=limit)

            pcache = CandleCache()

            # Initial load
            async with sf() as sess:
                first = await pcache.load_or_refresh(sess, "binance", f"DELAY{delay_minutes}/USDT", limit=limit)

            # Add candles simulating the delay
            total_offset = (limit - 1) * 3 + 3
            async with sf() as sess:
                await _add_new_candles(sess, "binance", f"DELAY{delay_minutes}/USDT",
                                       count=expected_new, start_offset=total_offset)

            # Refresh
            async with sf() as sess:
                second = await pcache.load_or_refresh(sess, "binance", f"DELAY{delay_minutes}/USDT", limit=limit)

            assert len(second) == limit, f"delay={delay_minutes}m: expected {limit}, got {len(second)}"

            # Check continuity: no gaps in timestamps
            tss = _candle_timestamps(second)
            for i in range(len(tss) - 1):
                gap = (tss[i + 1] - tss[i]).total_seconds()
                assert gap == 180, (
                    f"delay={delay_minutes}m: gap at index {i} is {gap}s, expected 180s"
                )

            await eng.dispose()
