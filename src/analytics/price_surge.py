"""
Детектор пампов по чистому росту цены (без объёмов и OI).

Используется для strategy_2. Проверяет только: выросла ли цена
на X% за Y минут. Не влияет на торговлю — только сигналы.
"""

import logging

import numpy as np

from src.analytics.base import BaseDetector, Signal
from src.analytics.data_provider import DataProvider
from src.analytics.utils import timeframe_to_minutes
from src.config import StrategyConfig

logger = logging.getLogger(__name__)


class PriceSurgeDetector(BaseDetector):
    """Детектор пампов: чистое движение цены за промежуток времени."""

    def __init__(
        self,
        config: StrategyConfig,
        timeframe: str = "3m",
        data_provider: DataProvider | None = None,
    ):
        self.config = config
        self._exclude_coins = set(c.upper() for c in config.exclude_coins)
        self._window_bars = max(
            config.price_surge_minutes // timeframe_to_minutes(timeframe), 1
        )
        self._dp = data_provider or DataProvider()

    @property
    def data_provider(self) -> DataProvider:
        return self._dp

    @data_provider.setter
    def data_provider(self, dp: DataProvider) -> None:
        self._dp = dp

    async def analyze(self, session) -> list[Signal]:
        if self.config.price_surge_pct <= 0:
            return []

        symbols = await self._dp.get_active_symbols(session, self._exclude_coins)
        if not symbols:
            return []

        signals = []
        for exchange, symbol in symbols:
            try:
                candles = await self._dp.load_candles(
                    session, exchange, symbol, self._window_bars + 5
                )
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
