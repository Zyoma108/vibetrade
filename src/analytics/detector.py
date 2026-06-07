import logging

import numpy as np

from src.analytics.base import BaseDetector, Signal
from src.analytics.data_provider import DataProvider
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct, timeframe_to_minutes
from src.config import StrategyConfig

logger = logging.getLogger(__name__)


class SetupDetector(BaseDetector):
    """Детектор сетапов: плавный рост объёмов + OI → начало пампа."""

    def __init__(
        self,
        config: StrategyConfig,
        timeframe: str = "3m",
        data_provider: DataProvider | None = None,
    ):
        self.config = config
        self._exclude_coins = set(c.upper() for c in config.exclude_coins)
        self._hour_bars = max(60 // timeframe_to_minutes(timeframe), 1)
        self._dp = data_provider or DataProvider()
        self._regime_volume_mult: float = 1.0  # Множитель от рыночного режима

    @property
    def data_provider(self) -> DataProvider:
        return self._dp

    @data_provider.setter
    def data_provider(self, dp: DataProvider) -> None:
        self._dp = dp

    @property
    def effective_volume_surge_mult(self) -> float:
        """Эффективный порог объёма с учётом рыночного режима."""
        return self.config.volume_surge_mult * self._regime_volume_mult

    def apply_regime_multiplier(self, mult: float) -> None:
        """Применить множитель рыночного режима к volume_surge_mult.
        Вызывается из Application каждый цикл перед analyze().
        mult = 1.0 для risk_on, 1.5 для cautious (изменяется через конфиг).
        """
        self._regime_volume_mult = mult

    async def analyze(self, session) -> list[Signal]:
        symbols = await self._dp.get_active_symbols(session, self._exclude_coins)
        if not symbols:
            return []

        signals = []
        seen = set()
        for exchange, symbol in symbols:
            try:
                limit = self.config.baseline_bars + self.config.sustain_bars + 10
                candles = await self._dp.load_candles(session, exchange, symbol, limit)
                if len(candles) < self.config.baseline_bars + self.config.sustain_bars:
                    continue

                if not self.check_volume_pattern(candles):
                    continue

                if not await self._check_oi_trend(session, exchange, symbol):
                    continue

                direction = self.check_price_trend(candles)
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
    # Volume pattern (public — used by backtest)
    # ------------------------------------------------------------------

    def check_volume_pattern(self, candles: list[dict]) -> bool:
        """Проверить плавный рост объёма над базовым уровнем."""
        volumes = np.array([c["volume"] for c in candles])

        baseline = np.median(volumes[:self.config.baseline_bars])
        if baseline <= 0:
            return False

        # Фильтр низколиквидных монет: объём в USDT ниже порога
        min_base_usdt = self.config.min_baseline_volume_usdt
        if min_base_usdt > 0:
            baseline_closes = np.array(
                [c["close"] for c in candles[:self.config.baseline_bars]]
            )
            median_price = np.median(baseline_closes)
            baseline_usdt = baseline * median_price
            if baseline_usdt < min_base_usdt:
                return False

        sustain = self.config.sustain_bars
        recent = volumes[-sustain:]

        # Все последние свечи должны быть выше порога
        threshold = baseline * self.effective_volume_surge_mult
        if not np.all(recent >= threshold):
            return False

        # Проверка на плавность: нет одиночного спайка
        recent_median = np.median(recent)
        if recent_median > 0:
            if np.max(recent) / recent_median > self.config.smooth_max_ratio:
                return False

            # Защита от свечи-выброса (distribution/climax):
            # объём последней свечи не должен превышать медиану
            # остальных sustain-свечей более чем в dump_volume_mult раз
            dump_mult = self.config.dump_volume_mult
            if dump_mult > 0 and len(recent) >= 2:
                others = recent[:-1]  # все свечи sustain, кроме последней
                others_median = np.median(others)
                if others_median > 0:
                    if recent[-1] / others_median > dump_mult:
                        logger.info(
                            f"Сигнал пропущен: свеча-выброс — объём последней "
                            f"свечи (x{recent[-1] / others_median:.1f}) "
                            f"> лимита x{dump_mult}"
                        )
                        return False

        return True

    # ------------------------------------------------------------------
    # Open Interest trend
    # ------------------------------------------------------------------

    async def _check_oi_trend(
        self, session, exchange: str, symbol: str
    ) -> bool:
        """Проверить, что OI растёт (приток денег, а не перекладка)."""
        oi_values = await self._dp.load_oi_values(
            session, exchange, symbol, OI_TREND_BARS
        )
        if oi_values is None:
            return False

        slope_pct = calculate_oi_slope_pct(np.array(oi_values))
        if slope_pct is None:
            return False

        return slope_pct >= self.config.oi_slope_min_pct

    # ------------------------------------------------------------------
    # Price direction (public — used by backtest)
    # ------------------------------------------------------------------

    def check_price_trend(self, candles: list[dict]) -> str | None:
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

        # Exhaustion filter: цена уже сильно выросла И свеча закрылась у верха
        # (покупатели выдохлись). Пропускаем только если свеча в середине/снизу —
        # это pullback в восходящем движении.
        ex_gain = self.config.exhaustion_gain_pct
        ex_pos = self.config.exhaustion_pos_ratio
        if ex_gain > 0 and change_pct > ex_gain:
            last_candle = candles[-1]
            candle_range = last_candle["high"] - last_candle["low"]
            if candle_range > 0:
                close_pos = (last_candle["close"] - last_candle["low"]) / candle_range
                if close_pos > ex_pos:
                    logger.info(
                        f"Сигнал пропущен: истощение — рост {change_pct:.1f}% "
                        f"(>{ex_gain}%) и свеча у верха (pos={close_pos:.2f} > {ex_pos})"
                    )
                    return None

        max_growth = self.config.price_growth_max_pct
        if max_growth > 0 and change_pct > max_growth:
            logger.info(
                f"Сигнал пропущен: рост {change_pct:.1f}% > лимита {max_growth}%"
            )
            return None

        return "long"

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def _build_signal(
        self, symbol: str, direction: str, candles: list[dict]
    ) -> Signal:
        sustain = self.config.sustain_bars
        volumes = [c["volume"] for c in candles[-sustain:]]
        baseline = np.median(
            [c["volume"] for c in candles[:self.config.baseline_bars]]
        )
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
