import logging
from datetime import datetime, timezone

import numpy as np

from src.analytics.base import BaseDetector, Signal
from src.analytics.data_provider import DataProvider
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct, timeframe_to_minutes
from src.config import StrategyConfig
from src.storage.models import FilteredSignal

logger = logging.getLogger(__name__)

# Стадии check_volume_pattern, при которых последняя свеча УЖЕ прошла порог
# всплеска, но её форма говорит о развороте/иссякании (не "ещё не доросла").
# Ретраить сдвигом здесь нельзя — это выбросило бы из окна ровно ту свечу,
# которую эти же проверки (и retracement/exhaustion-фильтры ниже, считающиеся
# на том же окне) должны ловить. См. анализ в памяти
# shift-compensation-narrowing (детектор-vs-бэктест).
VOLUME_REVERSAL_STAGES = frozenset({
    "volume_spike", "volume_dump", "volume_fading", "volume_declining",
})


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

    @staticmethod
    def _reject(context: dict | None, stage: str, reason: str) -> None:
        """Записать отказ фильтра: лог + `context` для `filtered_signals`.

        Один источник текста на оба назначения. Раньше каждый из 14 блоков отказа
        набирал одну и ту же фразу дважды — в `logger.info` и в `context["reason"]`,
        — и правка формулировки в одном месте расходила лог с тем, что попадало в
        `filtered_signals`, то есть с данными, по которым потом проводится аудит
        самих фильтров.
        """
        logger.info(f"Сигнал пропущен: {reason}")
        if context is not None:
            context["stage"] = stage
            context["reason"] = reason

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
                min_bars = self.config.baseline_bars + self.config.sustain_bars
                if len(candles) < min_bars:
                    continue

                # Проверяем volume pattern с lookback: если текущее окно не проходит —
                # пробуем на свечу раньше. Коллектор обновляет последнюю свечу по мере
                # её формирования (interval_seconds << timeframe, см.
                # MarketDataCollector._upsert_candles), поэтому в моменте скана самая
                # свежая свеча в окне часто ещё не закрыта и её объём занижен — это и
                # компенсирует сдвиг. НО: ретраим только если исходный отказ был
                # "тихим" (свеча ещё не доросла до порога всплеска, vol_ctx пуст) —
                # если отказ пришёл с explicit-причиной из VOLUME_REVERSAL_STAGES
                # (спайк/дамп/угасание/падение объёма), это значит свеча УЖЕ прошла
                # порог и её форма говорит о развороте — сдвиг тогда выбросил бы из
                # окна ровно ту свечу, которую эти же проверки (и retracement/
                # exhaustion-фильтры ниже, считающиеся на том же окне) должны ловить.
                vol_window = None
                vol_ctx: dict = {}
                if self.check_volume_pattern(candles, vol_ctx):
                    vol_window = candles
                elif vol_ctx.get("stage") not in VOLUME_REVERSAL_STAGES:
                    for shift in range(1, 2):  # -1 свеча
                        shifted = candles[:-shift]
                        shift_ctx: dict = {}
                        if len(shifted) >= min_bars and self.check_volume_pattern(shifted, shift_ctx):
                            vol_window = shifted
                            logger.info(
                                f"Сетап {symbol}: volume найден со сдвигом -{shift} свечей "
                                f"(исходный отказ: тихий — порог всплеска ещё не достигнут)"
                            )
                            break
                        if shift_ctx:
                            vol_ctx = shift_ctx  # причина последней попытки — самая релевантная

                if vol_window is None:
                    self._log_filtered(session, exchange, symbol, vol_ctx)
                    continue

                if self.config.oi_filter_enabled:
                    oi_ctx: dict = {}
                    if not await self._check_oi_trend(session, exchange, symbol, oi_ctx):
                        self._log_filtered(session, exchange, symbol, oi_ctx)
                        continue

                price_ctx: dict = {}
                direction = self.check_price_trend(vol_window, price_ctx)
                if direction is None:
                    self._log_filtered(session, exchange, symbol, price_ctx)
                    continue

                # Дедупликация: одна монета — один сигнал
                if symbol in seen:
                    continue
                seen.add(symbol)

                signal = self._build_signal(symbol, direction, vol_window)
                signals.append(signal)
                logger.info(f"Сетап найден: {symbol} {direction} ({exchange})")

            except Exception:
                logger.exception(f"Ошибка анализа {exchange}:{symbol}")

        return signals

    # ------------------------------------------------------------------
    # Filtered-signal audit trail (только для near-miss: сетап уже прошёл
    # основной порог всплеска объёма — сюда не попадают "тихие" монеты)
    # ------------------------------------------------------------------

    def _log_filtered(self, session, exchange: str, symbol: str, ctx: dict) -> None:
        if not ctx:
            return
        session.add(
            FilteredSignal(
                timestamp=datetime.now(timezone.utc),
                exchange=exchange,
                symbol=symbol,
                stage=ctx["stage"],
                reason=ctx["reason"],
            )
        )

    # ------------------------------------------------------------------
    # Volume pattern (public — used by backtest)
    # ------------------------------------------------------------------

    def check_volume_pattern(self, candles: list[dict], context: dict | None = None) -> bool:
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
                self._reject(
                    context,
                    "volume_spike",
                    f"одиночный спайк объёма: max/median={np.max(recent) / recent_median:.1f} "
                    f"> лимита x{self.config.smooth_max_ratio}",
                )
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
                        self._reject(
                            context,
                            "volume_dump",
                            f"свеча-выброс: объём последней свечи "
                            f"(x{recent[-1] / others_median:.1f}) > лимита x{dump_mult}",
                        )
                        return False

            # Проверка падения объёма в последней свече sustain:
            # если объём упал относительно среднего предыдущих — памп иссякает
            fading_ratio = self.config.volume_fading_ratio
            if fading_ratio > 0 and len(recent) >= 2:
                prev_avg = np.mean(recent[:-1])
                if prev_avg > 0 and recent[-1] / prev_avg < fading_ratio:
                    self._reject(
                        context,
                        "volume_fading",
                        f"объём последней свечи упал "
                        f"({recent[-1] / prev_avg:.1%} от среднего предыдущих)",
                    )
                    return False

            # Объём должен расти: последняя свеча sustain >= первой свечи sustain
            # (если объём падает — памп иссякает, сигнал не даём)
            if (
                self.config.volume_declining_enabled
                and len(recent) >= 2
                and recent[-1] < recent[0]
            ):
                self._reject(
                    context,
                    "volume_declining",
                    f"объём снижается — последняя свеча ({recent[-1]:.0f}) "
                    f"< первая ({recent[0]:.0f})",
                )
                return False

        return True

    # ------------------------------------------------------------------
    # Open Interest trend
    # ------------------------------------------------------------------

    async def _check_oi_trend(
        self, session, exchange: str, symbol: str, context: dict | None = None
    ) -> bool:
        """Проверить, что OI растёт (приток денег, а не перекладка)."""
        oi_values = await self._dp.load_oi_values(
            session, exchange, symbol, OI_TREND_BARS
        )
        if oi_values is None:
            return False

        # Если последняя точка OI ниже предпоследней — приток уже иссякает
        if (
            self.config.oi_declining_enabled
            and len(oi_values) >= 2
            and oi_values[-1] < oi_values[-2]
        ):
            self._reject(
                context,
                "oi_declining",
                "OI снижается — последняя точка ниже предпоследней",
            )
            return False

        slope_pct = calculate_oi_slope_pct(np.array(oi_values))
        if slope_pct is None:
            return False

        if slope_pct < self.config.oi_slope_min_pct:
            self._reject(
                context,
                "oi_slope_low",
                f"наклон OI {slope_pct:.1f}% < минимума {self.config.oi_slope_min_pct}%",
            )
            return False

        return True

    # ------------------------------------------------------------------
    # Price direction (public — used by backtest)
    # ------------------------------------------------------------------

    def check_price_trend(self, candles: list[dict], context: dict | None = None) -> str | None:
        """Только лонг: цена должна вырасти, но не слишком сильно
        (фильтр «памп уже состоялся»).
        Также защита от рагпулов: если за последний час падение > N%."""
        sustain = self.config.sustain_bars
        all_closes = np.array([c["close"] for c in candles])
        opens = np.array([c["open"] for c in candles[-sustain:]])
        closes = np.array([c["close"] for c in candles[-sustain:]])

        if opens[0] <= 0:
            return None

        # Pre-sustain pump filter: проверяем рост за 10 свечей (30 мин) до sustain-окна.
        # Блокирует сигнал, если монета уже сильно выросла ДО начала sustain-периода.
        pre_surge_max = self.config.pre_surge_max_pct
        if pre_surge_max > 0:
            pre_sustain_bars = self.config.pre_surge_bars
            min_required = self.config.baseline_bars + self.config.sustain_bars + pre_sustain_bars
            if len(candles) >= min_required:
                pre_start_idx = -(self.config.sustain_bars + pre_sustain_bars)
                pre_end_idx = -self.config.sustain_bars
                pre_open = candles[pre_start_idx]['open']
                pre_close = candles[pre_end_idx - 1]['close'] if pre_end_idx < 0 else candles[pre_end_idx]['close']
                if pre_open > 0:
                    pre_pump_pct = (pre_close / pre_open - 1) * 100
                    if pre_pump_pct > pre_surge_max:
                        self._reject(
                            context,
                            "pre_surge_pump",
                            f"предварительный памп {pre_pump_pct:.1f}% "
                            f"(>{pre_surge_max}%) за 30 мин до sustain-окна",
                        )
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
                        self._reject(
                            context,
                            "hourly_drop",
                            f"падение {drop:.1f}% за час (лимит {-max_drop}%)",
                        )
                        return None

        change_pct = (closes[-1] / opens[0] - 1) * 100

        if change_pct < self.config.price_growth_min_pct:
            self._reject(
                context,
                "price_growth_low",
                f"рост цены {change_pct:.1f}% < минимума {self.config.price_growth_min_pct}%",
            )
            return None

        # Window-range filter: насколько широко ходила цена внутри sustain-окна.
        # stop_loss_pct — фиксированный процент, поэтому у монеты с размахом 5-7%
        # за 12 минут стоп стоит внутри обычного шума и выбивается почти
        # гарантированно, независимо от того, куда пойдёт движение дальше.
        # См. max_window_range_pct в StrategyConfig (аудит августа 2026).
        max_range = self.config.max_window_range_pct
        if max_range > 0:
            win_high = np.max([c["high"] for c in candles[-sustain:]])
            win_low = np.min([c["low"] for c in candles[-sustain:]])
            if win_low > 0:
                range_pct = (win_high / win_low - 1) * 100
                if range_pct > max_range:
                    self._reject(
                        context,
                        "window_range",
                        f"размах sustain-окна {range_pct:.1f}% (>{max_range}%) — "
                        f"фиксированный стоп внутри шума",
                    )
                    return None

        # Exhaustion filter v1: цена выросла за sustain-окно И свеча закрылась у верха
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
                    self._reject(
                        context,
                        "exhaustion",
                        f"истощение — рост {change_pct:.1f}% (>{ex_gain}%) "
                        f"и свеча у верха (pos={close_pos:.2f} > {ex_pos})",
                    )
                    return None

        # Exhaustion filter v2: экстремальный памп от baseline (pump-and-dump).
        # Если цена улетала выше медианы baseline больше чем на
        # exhaustion_extreme_pct — блокируем независимо от close_pos. Ловит случаи,
        # когда памп и дамп произошли до/внутри sustain-окна и последняя свеча уже
        # развернулась. Порог отдельный от exhaustion_gain_pct (v1): фильтры меряют
        # разное — v1 форму последней свечи, v2 удалённость от baseline — и их
        # оптимумы не связаны (раньше v2 был захардкожен как ex_gain * 6 = 30%).
        extreme_threshold = self.config.exhaustion_extreme_pct
        if extreme_threshold > 0:
            baseline_closes = np.array(
                [c["close"] for c in candles[: self.config.baseline_bars]]
            )
            baseline_median = np.median(baseline_closes)
            recent_highs = np.array([c["high"] for c in candles[-sustain:]])
            extreme_pump_pct = (np.max(recent_highs) / baseline_median - 1) * 100
            if extreme_pump_pct > extreme_threshold:
                self._reject(
                    context,
                    "exhaustion_extreme",
                    f"экстремальный памп {extreme_pump_pct:.1f}% от baseline "
                    f"(>{extreme_threshold:.0f}%) — вероятный pump-and-dump",
                )
                return None

        # Retracement filter: сколько цена уже откатилась от пика (high) sustain-окна
        # к моменту закрытия последней свечи. Ловит случаи, когда движение уже
        # развернулось, ПОКА набиралось подтверждение объёма (sustain_bars=4 свечи =
        # 12 минут) — по конструкции детектор видит только суммарный рост окна
        # (change_pct = closes[-1]/opens[0]), а не его форму, и может пропустить
        # сигнал уже на нисходящем участке. См. db-audit-august-2026: 92% живых
        # лоссов на алго-пути ни разу не доходили даже до частичной фиксации +3.5%,
        # средний вход — на 1.93% ниже недавнего пика. Выключен по умолчанию (0) —
        # включать только после свипа порога на исторических данных.
        max_retracement = self.config.max_window_retracement_pct
        if max_retracement > 0:
            window_high = np.max([c["high"] for c in candles[-sustain:]])
            if window_high > 0:
                retracement_pct = (window_high - closes[-1]) / window_high * 100
                if retracement_pct > max_retracement:
                    self._reject(
                        context,
                        "retracement",
                        f"откат {retracement_pct:.1f}% от пика sustain-окна "
                        f"(>{max_retracement}%) — движение уже развернулось",
                    )
                    return None

        max_growth = self.config.price_growth_max_pct
        if max_growth > 0 and change_pct > max_growth:
            self._reject(
                context,
                "price_growth_high",
                f"рост {change_pct:.1f}% > лимита {max_growth}%",
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
            confidence=min(round(surge * self.config.confidence_surge_mult), 100),
            message=(
                f"Объём: x{surge:.1f} от нормы\n"
                f"Последние {sustain} свечей выше порога\n"
                f"Цена: {candles[-1]['close']:.6f}"
            ),
        )
