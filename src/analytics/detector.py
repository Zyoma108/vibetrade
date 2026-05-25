import logging

import numpy as np
from sqlalchemy import desc, select

from src.analytics.base import BaseDetector, Signal
from src.config import StrategyConfig
from src.storage.models import Candle, OpenInterest

logger = logging.getLogger(__name__)

# Фиксированные параметры (редко меняются — не в конфиге)
SMOOTH_MAX_RATIO = 5.0   # макс. отношение макс/медиана объёма в окне (отсекает спайки)
OI_TREND_BARS = 3        # сколько последних точек OI для проверки тренда


class SetupDetector(BaseDetector):
    """Детектор сетапов: плавный рост объёмов + OI → начало пампа."""

    def __init__(self, config: StrategyConfig, timeframe: str = "3m"):
        self.config = config
        self._exclude_coins = set(c.upper() for c in config.exclude_coins)
        # Количество свечей в часе
        if timeframe.endswith("m"):
            tf_min = int(timeframe[:-1])
        elif timeframe.endswith("h"):
            tf_min = int(timeframe[:-1]) * 60
        else:
            tf_min = 3
        self._hour_bars = max(60 // tf_min, 1)

    async def analyze(self, session) -> list[Signal]:
        symbols = await self._get_active_symbols(session)
        if not symbols:
            return []

        signals = []
        seen = set()
        for exchange, symbol in symbols:
            try:
                candles = await self._load_candles(session, exchange, symbol)
                if len(candles) < self.config.baseline_bars + self.config.sustain_bars:
                    continue

                if not self._check_volume_pattern(candles):
                    continue

                if not await self._check_oi_trend(session, exchange, symbol):
                    continue

                direction = self._check_price_trend(candles)
                if direction is None:
                    continue

                # Дедупликация: одна монета — один сигнал
                if symbol in seen:
                    continue
                seen.add(symbol)

                signal = self._build_signal(symbol, direction, candles)
                signals.append(signal)
                logger.info(f"Сетап найден: {symbol} {direction} ({exchange})")

            except Exception:
                logger.exception(f"Ошибка анализа {exchange}:{symbol}")

        return signals

    # ------------------------------------------------------------------
    # Volume pattern
    # ------------------------------------------------------------------

    def _check_volume_pattern(self, candles: list[dict]) -> bool:
        """Проверить плавный рост объёма над базовым уровнем."""
        volumes = np.array([c["volume"] for c in candles])

        baseline = np.median(volumes[:self.config.baseline_bars])
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

    async def _check_oi_trend(self, session, exchange: str, symbol: str) -> bool:
        """Проверить, что OI растёт (приток денег, а не перекладка)."""
        stmt = (
            select(OpenInterest)
            .where(
                OpenInterest.exchange == exchange,
                OpenInterest.symbol == symbol,
            )
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
        return slope_pct >= self.config.oi_slope_min_pct

    # ------------------------------------------------------------------
    # Price direction
    # ------------------------------------------------------------------

    def _check_price_trend(self, candles: list[dict]) -> str | None:
        """Только лонг: цена должна вырасти, но не слишком сильно
        (фильтр «памп уже состоялся»).
        Также защита от рагпулов: если за последний час падение > N%."""
        sustain = self.config.sustain_bars
        all_closes = np.array([c["close"] for c in candles])
        opens = np.array([c["open"] for c in candles[-sustain:]])
        closes = np.array([c["close"] for c in candles[-sustain:]])

        if opens[0] <= 0:
            return None

        # Защита от рагпулов: падение за последний час
        max_drop = self.config.max_hourly_drop_pct
        if max_drop > 0:
            if len(all_closes) >= self._hour_bars:
                recent_low = np.min(all_closes[-self._hour_bars:])
                ref_price = all_closes[-self._hour_bars]
                if ref_price > 0:
                    drop = (recent_low / ref_price - 1) * 100
                    if drop <= -max_drop:
                        logger.info(
                            f"Сигнал пропущен: падение {drop:.1f}% за час "
                            f"(лимит {-max_drop}%)"
                        )
                        return None

        change_pct = (closes[-1] / opens[0] - 1) * 100

        if change_pct < self.config.price_growth_min_pct:
            return None

        max_growth = self.config.price_growth_max_pct
        if max_growth > 0 and change_pct > max_growth:
            logger.debug(f"Сигнал пропущен: рост {change_pct:.1f}% > лимита {max_growth}%")
            return None

        return "long"

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    async def _get_active_symbols(self, session) -> list[tuple[str, str]]:
        """Все пары (exchange, symbol), которые есть на торгуемой бирже (ByBit)."""
        # Символы, доступные на ByBit
        bybit_syms_stmt = (
            select(Candle.symbol)
            .where(Candle.exchange == "bybit")
            .distinct()
        )
        bybit_result = await session.execute(bybit_syms_stmt)
        bybit_symbols = set(bybit_result.scalars().all())

        # Все уникальные пары биржа+символ
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

    async def _load_candles(
        self, session, exchange: str, symbol: str
    ) -> list[dict]:
        """Загрузить последние свечи для пары биржа+символ."""
        limit = self.config.baseline_bars + self.config.sustain_bars + 10
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
            if r.volume > 0  # пропускаем незакрытые свечи (volume=0)
        ]

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def _build_signal(
        self, symbol: str, direction: str, candles: list[dict]
    ) -> Signal:
        sustain = self.config.sustain_bars
        volumes = [c["volume"] for c in candles[-sustain:]]
        baseline = np.median([c["volume"] for c in candles[:self.config.baseline_bars]])
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
