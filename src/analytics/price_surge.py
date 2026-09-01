"""
Детектор пампов по чистому росту цены (без объёмов и OI).

Используется для strategy_2. Проверяет только: выросла ли цена
на X% за Y минут. Не влияет на торговлю — только сигналы.
"""

import logging
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import BaseDetector, Signal
from src.analytics.data_provider import DataProvider
from src.analytics.utils import timeframe_to_minutes
from src.config import StrategyConfig
from src.storage.models import PriceSurgeSignal

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

        recent = await self._recent_alerts(session)

        # Лучший (максимальный) рост по монете за цикл. Ключ — символ, а не пара
        # exchange:symbol: одна и та же монета торгуется и на binance, и на bybit,
        # и без схлопывания по символу за цикл уходило бы два одинаковых сообщения.
        best: dict[str, tuple[float, float]] = {}
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
                    prev = best.get(symbol)
                    if prev is None or change_pct > prev[0]:
                        best[symbol] = (change_pct, float(closes[-1]))

            except Exception:
                logger.exception(f"Ошибка анализа {exchange}:{symbol}")

        signals: list[Signal] = []
        suppressed = 0
        for symbol, (change_pct, close) in best.items():
            if not self._passes_cooldown(symbol, change_pct, recent):
                suppressed += 1
                continue

            signals.append(
                Signal(
                    symbol=symbol,
                    setup_type="price_surge",
                    direction="long",
                    confidence=min(round(change_pct * 5), 95),
                    message=(
                        f"Рост цены: +{change_pct:.1f}% за "
                        f"{self.config.price_surge_minutes} мин\n"
                        f"Цена: {close:.6f}"
                    ),
                )
            )
            logger.info(f"Памп: {symbol} +{change_pct:.1f}%")

        if suppressed:
            logger.info(f"Пампы в кулдауне (сигнал не повторён): {suppressed}")

        return signals

    # ------------------------------------------------------------------
    # Кулдаун
    # ------------------------------------------------------------------

    async def _recent_alerts(self, session: AsyncSession) -> dict[str, float]:
        """Максимальный уже отправленный рост по монете внутри окна кулдауна.

        Источник — таблица `price_surge_signals`, а не память процесса: окно
        детектора (`price_surge_minutes`) в разы длиннее цикла сканирования,
        поэтому один и тот же памп попадает в него на каждом цикле, и после
        ускорения цикла до ~45с одна монета давала до двух десятков одинаковых
        сообщений. Состояние в БД переживает рестарт бота — иначе спам
        возобновлялся бы после каждого деплоя.
        """
        minutes = self.config.price_surge_cooldown_minutes
        if minutes <= 0:
            return {}

        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)
        rows = await session.execute(
            select(PriceSurgeSignal.symbol, func.max(PriceSurgeSignal.change_pct))
            .where(PriceSurgeSignal.timestamp >= cutoff)
            .group_by(PriceSurgeSignal.symbol)
        )
        return {symbol: pct for symbol, pct in rows.all()}

    def _passes_cooldown(
        self, symbol: str, change_pct: float, recent: dict[str, float],
    ) -> bool:
        """Пропустить сигнал, если по монете недавно уже был такой же памп.

        Исключение — эскалация: памп разогнался ещё на
        `price_surge_escalation_pct` пунктов сверх максимума, о котором уже
        сообщили. Порог считается от максимума в окне, а не от последнего
        сообщения, поэтому планка только растёт и «пила» вокруг одного уровня
        не пробивает кулдаун.
        """
        alerted = recent.get(symbol)
        if alerted is None:
            return True

        escalation = self.config.price_surge_escalation_pct
        return escalation > 0 and change_pct >= alerted + escalation
