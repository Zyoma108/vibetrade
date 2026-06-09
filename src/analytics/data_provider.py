"""
Centralized data-loading layer with in-memory cache for one cycle.

Creates a single DataProvider per cycle to avoid double DB queries from
multiple detectors requesting the same candles/symbols.

Instance-scoped — create a new DataProvider each cycle, throw it away.
No TTL management needed.

CandleCache is a persistent companion that survives across cycles:
first load fetches full history, subsequent cycles only fetch new candles.
"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.storage.models import Candle, OpenInterest, Ticker

logger = logging.getLogger(__name__)


class CandleCache:
    """Persistent candle cache across collection cycles.

    On first request for a symbol: loads full history (up to ``limit`` bars).
    On subsequent requests: loads only candles newer than the latest cached
    timestamp, appends them, and trims to ``limit``.
    """

    def __init__(self) -> None:
        self._candles: dict[str, list[dict[str, Any]]] = {}
        # Latest cached timestamp per key — used to fetch only the delta
        self._max_ts: dict[str, datetime] = {}

    async def load_or_refresh(
        self, session: AsyncSession, exchange: str, symbol: str, limit: int,
    ) -> list[dict[str, Any]]:
        """Return up to ``limit`` most recent candles for a pair.

        Uses cached data when available, only fetching candles that arrived
        since the last cached timestamp.
        """
        cache_key = f"{exchange}:{symbol}"
        cached = self._candles.get(cache_key)

        if cached is not None and len(cached) >= limit:
            # We have enough — only fetch new candles since the last one
            new_candles = await self._fetch_newer_than(
                session, exchange, symbol, self._max_ts[cache_key],
            )
            if new_candles:
                cached.extend(new_candles)
                self._max_ts[cache_key] = cached[-1]["timestamp"]  # type: ignore[typeddict-item]
                # Trim to limit
                if len(cached) > limit:
                    self._candles[cache_key] = cached[-limit:]
            return self._candles[cache_key][-limit:]

        # First load — full history, or cache had fewer bars than requested
        candles = await self._fetch_recent(session, exchange, symbol, limit)
        if candles:
            self._candles[cache_key] = candles
            self._max_ts[cache_key] = candles[-1]["timestamp"]  # type: ignore[typeddict-item]
        else:
            self._candles[cache_key] = []
        # Return a copy to prevent mutation by external code
        return list(self._candles[cache_key])

    @staticmethod
    async def _fetch_recent(
        session: AsyncSession, exchange: str, symbol: str, limit: int,
    ) -> list[dict[str, Any]]:
        """Load the most recent ``limit`` candles from the DB."""
        stmt = (
            select(Candle)
            .where(Candle.exchange == exchange, Candle.symbol == symbol)
            .order_by(desc(Candle.timestamp))
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [
            {
                "open": r.open, "high": r.high, "low": r.low,
                "close": r.close, "volume": r.volume,
                "timestamp": r.timestamp,
            }
            for r in reversed(rows)
            if r.volume > 0
        ]

    @staticmethod
    async def _fetch_newer_than(
        session: AsyncSession, exchange: str, symbol: str, since: datetime,
    ) -> list[dict[str, Any]]:
        """Load candles with timestamp strictly greater than ``since``."""
        stmt = (
            select(Candle)
            .where(
                Candle.exchange == exchange,
                Candle.symbol == symbol,
                Candle.timestamp > since,
            )
            .order_by(Candle.timestamp)  # chronological — oldest first
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()
        return [
            {
                "open": r.open, "high": r.high, "low": r.low,
                "close": r.close, "volume": r.volume,
                "timestamp": r.timestamp,
            }
            for r in rows
            if r.volume > 0
        ]


class DataProvider:
    """Loads and caches market data within a single collection cycle.

    If a ``CandleCache`` is provided, candle loads are persistent across
    cycles: only new candles are fetched from the DB on each call.
    """

    def __init__(self, candle_cache: CandleCache | None = None) -> None:
        self._symbols: list[tuple[str, str]] | None = None
        self._candles: dict[str, list[dict[str, Any]]] = {}
        self._oi_values: dict[str, list[float]] = {}
        self._persistent_cache = candle_cache

    # ------------------------------------------------------------------
    # Active symbols
    # ------------------------------------------------------------------

    async def get_active_symbols(
        self, session: AsyncSession, exclude_coins: set[str]
    ) -> list[tuple[str, str]]:
        """Return all (exchange, symbol) pairs traded on ByBit (with candle data)."""
        if self._symbols is not None:
            return [
                (ex, sym) for ex, sym in self._symbols
                if sym.split("/")[0].upper() not in exclude_coins
            ]

        # Symbols present on ByBit (from tickers)
        bybit_result = await session.execute(
            select(Ticker.symbol).where(Ticker.exchange == "bybit").distinct()
        )
        bybit_symbols = set(bybit_result.scalars().all())

        # All unique exchange+symbol pairs with candle data
        result = await session.execute(
            select(Candle.exchange, Candle.symbol)
            .distinct()
            .order_by(Candle.exchange, Candle.symbol)
        )
        self._symbols = [
            (ex, sym)
            for ex, sym in result.all()
            if sym in bybit_symbols
        ]

        return [
            (ex, sym) for ex, sym in self._symbols
            if sym.split("/")[0].upper() not in exclude_coins
        ]

    # ------------------------------------------------------------------
    # Candles
    # ------------------------------------------------------------------

    async def load_candles(
        self, session: AsyncSession, exchange: str, symbol: str, limit: int,
    ) -> list[dict[str, Any]]:
        """Load OHLCV candles for a pair, caching within the cycle.

        When a persistent ``CandleCache`` is available, only fetches
        new candles since the last cached timestamp on the first call
        within a cycle.  Subsequent calls within the same cycle hit
        the intra-cycle cache (shared across detectors).

        Returns candles in chronological order (oldest first), skipping
        zero-volume bars (unclosed candles).
        """
        cache_key = f"{exchange}:{symbol}"

        # Intra-cycle cache — already loaded this symbol during this cycle
        if cache_key in self._candles and len(self._candles[cache_key]) >= limit:
            return self._candles[cache_key][-limit:]

        if self._persistent_cache is not None:
            candles = await self._persistent_cache.load_or_refresh(
                session, exchange, symbol, limit,
            )
            # Populate intra-cycle cache so multiple detectors share it
            # (strip 'timestamp' — detector code expects only OHLCV keys)
            self._candles[cache_key] = [
                {"open": c["open"], "high": c["high"], "low": c["low"],
                 "close": c["close"], "volume": c["volume"]}
                for c in candles
            ]
            return self._candles[cache_key]

        # No persistent cache — load directly from DB (backtest / tests)
        stmt = (
            select(Candle)
            .where(Candle.exchange == exchange, Candle.symbol == symbol)
            .order_by(desc(Candle.timestamp))
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

        candles = [
            {
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in reversed(rows)  # chronological order
            if r.volume > 0  # skip unclosed candles
        ]
        self._candles[cache_key] = candles
        return candles

    # ------------------------------------------------------------------
    # Open Interest
    # ------------------------------------------------------------------

    async def load_oi_values(
        self, session: AsyncSession, exchange: str, symbol: str, n_bars: int,
    ) -> list[float] | None:
        """Load last N OI values for a pair in chronological order.

        Returns None if fewer than n_bars records exist.
        """
        cache_key = f"oi:{exchange}:{symbol}"
        if cache_key in self._oi_values:
            oi_vals = self._oi_values[cache_key]
            if len(oi_vals) >= n_bars:
                return oi_vals[-n_bars:]

        stmt = (
            select(OpenInterest.value)
            .where(
                OpenInterest.exchange == exchange,
                OpenInterest.symbol == symbol,
            )
            .order_by(desc(OpenInterest.timestamp))
            .limit(n_bars)
        )
        result = await session.execute(stmt)
        oi_values = list(result.scalars().all())

        if len(oi_values) < n_bars:
            self._oi_values[cache_key] = oi_values
            return None

        # Reverse to chronological order
        oi_chronological = list(reversed(oi_values))
        self._oi_values[cache_key] = oi_chronological
        return oi_chronological
