import logging

import numpy as np
from sqlalchemy import desc, select

from src.analytics.base import BaseDetector, Signal
from src.config import StrategyConfig
from src.storage.models import Candle, OpenInterest

logger = logging.getLogger(__name__)

# Фиксированные параметры (редко меняются — не в конфиге)
SMOOTH_MAX_RATIO = 5.0   # макс. отношение макс/медиана объёма в окне (отсекает спайки)
OI_TREND_BARS = 12       # сколько последних точек OI для проверки тренда
PRICE_TREND_BARS = 12    # сколько последних свечей для проверки направления цены


class SetupDetector(BaseDetector):
    """Детектор сетапов: плавный рост объёмов + OI → начало пампа."""

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.baseline_bars = 50  # свечей для расчёта "нормального" объёма

    async def analyze(self, session) -> list[Signal]:
        symbols = await self._get_active_symbols(session)
        if not symbols:
            return []

        signals = []
        for exchange, symbol in symbols:
            try:
                candles = await self._load_candles(session, exchange, symbol)
                if len(candles) < self.baseline_bars + self.config.sustain_bars:
                    continue

                if not self._check_volume_pattern(candles):
                    continue

                if not self._check_oi_trend(session, symbol):
                    continue

                direction = self._check_price_trend(candles)
                if direction is None:
                    continue

                name = f"{exchange}:{symbol}"
                signal = self._build_signal(name, direction, candles)
                signals.append(signal)
                logger.info(f"Сетап найден: {name} {direction}")

            except Exception:
                logger.exception(f"Ошибка анализа {exchange}:{symbol}")

        return signals

    # ------------------------------------------------------------------
    # Volume pattern
    # ------------------------------------------------------------------

    def _check_volume_pattern(self, candles: list[dict]) -> bool:
        """Проверить плавный рост объёма над базовым уровнем."""
        volumes = np.array([c["volume"] for c in candles])

        baseline = np.median(volumes[:self.baseline_bars])
        if baseline <= 0:
            return False

        sustain = self.config.sustain_bars
        recent = volumes[-sustain:]

        # Все последние свечи должны быть выше порога
        threshold = baseline * self.config.volume_surge_mult
        if not np.all(recent >= threshold):
            return False

        # Проверка на плавность: нет одиночного спайка
        recent_median = np.median(recent)
        if recent_median > 0:
            if np.max(recent) / recent_median > SMOOTH_MAX_RATIO:
                return False

        # Объём не должен резко падать в последней свече
        if recent[-1] < np.mean(recent[:2]) * 0.5:
            return False

        return True

    # ------------------------------------------------------------------
    # Open Interest trend
    # ------------------------------------------------------------------

    async def _check_oi_trend(self, session, symbol: str) -> bool:
        """Проверить, что OI растёт (приток денег, а не перекладка)."""
        stmt = (
            select(OpenInterest)
            .where(OpenInterest.symbol == symbol)
            .order_by(desc(OpenInterest.timestamp))
            .limit(OI_TREND_BARS)
        )
        result = await session.execute(stmt)
        rows = result.scalars().all()

        if len(rows) < OI_TREND_BARS:
            return False

        values = np.array([r.value for r in reversed(rows)])

        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]

        mean_oi = np.mean(values)
        if mean_oi <= 0:
            return False

        slope_pct = (slope * len(values)) / mean_oi * 100
        return slope_pct > 0

    # ------------------------------------------------------------------
    # Price direction
    # ------------------------------------------------------------------

    def _check_price_trend(self, candles: list[dict]) -> str | None:
        """Определить направление цены. None — нет явного тренда."""
        closes = np.array([c["close"] for c in candles[-PRICE_TREND_BARS:]])

        x = np.arange(len(closes))
        slope = np.polyfit(x, closes, 1)[0]

        mean_price = np.mean(closes)
        if mean_price <= 0:
            return None

        slope_pct = (slope * len(closes)) / mean_price * 100

        if slope_pct > 0.2:
            return "long"
        elif slope_pct < -0.2:
            return "short"
        return None

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    async def _get_active_symbols(self, session) -> list[tuple[str, str]]:
        """Все пары (exchange, symbol), по которым есть свежие данные."""
        stmt = (
            select(Candle.exchange, Candle.symbol)
            .distinct()
            .order_by(Candle.exchange, Candle.symbol)
        )
        result = await session.execute(stmt)
        return [(row[0], row[1]) for row in result.all()]

    async def _load_candles(
        self, session, exchange: str, symbol: str
    ) -> list[dict]:
        """Загрузить последние свечи для пары биржа+символ."""
        limit = self.baseline_bars + self.config.sustain_bars + 10
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
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
            }
            for r in reversed(rows)  # хронологический порядок
        ]

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def _build_signal(
        self, symbol: str, direction: str, candles: list[dict]
    ) -> Signal:
        sustain = self.config.sustain_bars
        volumes = [c["volume"] for c in candles[-sustain:]]
        baseline = np.median([c["volume"] for c in candles[:self.baseline_bars]])
        surge = np.mean(volumes) / baseline if baseline > 0 else 0

        return Signal(
            symbol=symbol,
            setup_type="volume_surge",
            direction=direction,
            confidence=min(round(surge * 20), 95),
            message=(
                f"Объём: x{surge:.1f} от нормы\n"
                f"Последние {sustain} свечей выше порога\n"
                f"Цена: {candles[-1]['close']:.6f}"
            ),
        )
