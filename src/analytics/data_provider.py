"""
Centralized data-loading layer with in-memory cache for one cycle.

Creates a single DataProvider per cycle to avoid double DB queries from
multiple detectors requesting the same candles/symbols.

Instance-scoped — create a new DataProvider each cycle, throw it away.
No TTL management needed.
"""

import logging
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.storage.models import Candle, OpenInterest, Ticker

logger = logging.getLogger(__name__)


class DataProvider:
    """Loads and caches market data within a single collection cycle."""

    def __init__(self) -> None:
        self._symbols: list[tuple[str, str]] | None = None
        self._candles: dict[str, list[dict[str, Any]]] = {}
        self._oi_values: dict[str, list[float]] = {}

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

        Returns candles in chronological order (oldest first), skipping
        zero-volume bars (unclosed candles).
        """
        cache_key = f"{exchange}:{symbol}"
        if cache_key in self._candles and len(self._candles[cache_key]) >= limit:
            return self._candles[cache_key][-limit:]

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
