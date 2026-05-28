"""
Детектор пампов по чистому росту цены (без объёмов и OI).

Используется для strategy_2. Проверяет только: выросла ли цена
на X% за Y минут. Не влияет на торговлю — только сигналы.
"""

import logging

import numpy as np
from sqlalchemy import desc, select

from src.analytics.base import BaseDetector, Signal
from src.config import StrategyConfig
from src.storage.models import Candle

logger = logging.getLogger(__name__)


class PriceSurgeDetector(BaseDetector):
    """Детектор пампов: чистое движение цены за промежуток времени."""

    def __init__(self, config: StrategyConfig, timeframe: str = "3m"):
        self.config = config
        self._exclude_coins = set(c.upper() for c in config.exclude_coins)
        # Сколько свечей в промежутке
        if timeframe.endswith("m"):
            tf_min = int(timeframe[:-1])
        elif timeframe.endswith("h"):
            tf_min = int(timeframe[:-1]) * 60
        else:
            tf_min = 3
        self._window_bars = max(config.price_surge_minutes // tf_min, 1)

    async def analyze(self, session) -> list[Signal]:
        if self.config.price_surge_pct <= 0:
            return []

        symbols = await self._get_active_symbols(session)
        if not symbols:
            return []

        signals = []
        for exchange, symbol in symbols:
            try:
                candles = await self._load_candles(session, exchange, symbol)
                if len(candles) < self._window_bars + 1:
                    continue

                opens = np.array([c["open"] for c in candles[-self._window_bars:]])
                closes = np.array([c["close"] for c in candles[-self._window_bars:]])
                if opens[0] <= 0:
                    continue

                change_pct = (closes[-1] / opens[0] - 1) * 100
                if change_pct >= self.config.price_surge_pct:
                    signal = Signal(
                        symbol=symbol,
                        setup_type="price_surge",
                        direction="long",
                        confidence=min(round(change_pct * 5), 95),
                        message=(
                            f"Рост цены: +{change_pct:.1f}% за "
                            f"{self.config.price_surge_minutes} мин\n"
                            f"Цена: {closes[-1]:.6f}"
                        ),
                    )
                    signals.append(signal)
                    logger.info(f"Памп: {symbol} +{change_pct:.1f}%")

            except Exception:
                logger.exception(f"Ошибка анализа {exchange}:{symbol}")

        return signals

    async def _get_active_symbols(self, session) -> list[tuple[str, str]]:
        """Все пары (exchange, symbol) с данными."""
        # Символы, доступные на ByBit
        bybit_stmt = (
            select(Candle.symbol)
            .where(Candle.exchange == "bybit")
            .distinct()
        )
        bybit_result = await session.execute(bybit_stmt)
        bybit_symbols = set(bybit_result.scalars().all())

        stmt = (
            select(Candle.exchange, Candle.symbol)
            .distinct()
            .order_by(Candle.exchange, Candle.symbol)
        )
        result = await session.execute(stmt)
        return [
            (ex, sym)
            for ex, sym in result.all()
            if sym in bybit_symbols
            and sym.split("/")[0].upper() not in self._exclude_coins
        ]

    async def _load_candles(self, session, exchange: str, symbol: str) -> list[dict]:
        limit = self._window_bars + 5
        stmt = (
            select(Candle)
            .where(Candle.exchange == exchange, Candle.symbol == symbol)
            .order_by(desc(Candle.timestamp))
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

        return [
            {"open": r.open, "high": r.high, "low": r.low,
             "close": r.close, "volume": r.volume}
            for r in reversed(rows)
            if r.volume > 0
        ]
