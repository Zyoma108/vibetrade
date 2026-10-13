"""
Processor for price surge signals — enrichment, DB persistence, and notifications.

Extracted from Application._on_collect_cycle_done to keep the orchestrator lean.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.data_provider import DataProvider
from src.analytics.price_surge import PriceSurgeDetector
from src.analytics.utils import timeframe_to_minutes
from src.config import StrategyConfig
from src.storage.models import Candle, OpenInterest, PriceSurgeSignal

logger = logging.getLogger(__name__)


class PriceSurgeSignalProcessor:
    """Обогащает сигналы пампа метриками и готовит тексты сообщений.

    Отправкой не занимается: она сетевая, а обработка идёт внутри транзакции
    цикла сбора — см. `process()`."""

    def __init__(
        self,
        config: StrategyConfig,
        detector: PriceSurgeDetector,
        timeframe: str = "3m",
        data_provider: DataProvider | None = None,
        exchanges: list[str] | None = None,
    ):
        self.config = config
        self.detector = detector
        self.timeframe = timeframe
        self._dp = data_provider or DataProvider()
        # Список бирж нужен, чтобы запросы к open_interest попадали в составной
        # индекс (exchange, symbol, timestamp). Без него запрос «по любой бирже»
        # требовал отдельного индекса по symbol — 236 МБ и 80% времени цикла
        # сбора OI на его обслуживании (замер 22.09.2026).
        self._exchanges = exchanges or ["bybit", "binance"]

    @property
    def data_provider(self) -> DataProvider:
        return self._dp

    @data_provider.setter
    def data_provider(self, dp: DataProvider) -> None:
        self._dp = dp
        self.detector.data_provider = dp

    async def process(self, session: AsyncSession) -> list[str]:
        """Прогнать детектор, обогатить сигналы и записать в БД.

        Возвращает тексты сообщений, но САМ ИХ НЕ ШЛЁТ: отправка идёт по сети, а
        метод вызывается изнутри транзакции цикла сбора — раньше write-лок SQLite
        удерживался на время HTTP-запроса к Telegram. Вызывающий код отправляет
        результат после `commit()` (см. `Application._flush_outbox`).
        """
        signals = await self.detector.analyze(session)

        if not signals:
            return []

        outbox: list[str] = []

        interval = self.config.price_surge_minutes

        # Pre-compute hour_bars from timeframe
        tf_min = timeframe_to_minutes(self.timeframe)
        hour_bars = max(60 // tf_min, 1)

        for sig in signals:
            # Рост и цены — ровно те, по которым детектор принял решение. Раньше
            # их пересчитывали здесь запросом по символу без фильтра биржи: у
            # монеты с рядами на binance и bybit `limit 7` брал бары обеих бирж
            # вперемешку, и в БД попадал рост вплоть до отрицательного. Кулдаун
            # сравнивает новые пампы с максимумом из БД, поэтому порог эскалации
            # падал ниже нуля, и сигнал повторялся каждый цикл (13.10.2026).
            change_pct = sig.change_pct
            open_p = sig.window_open
            close_p = sig.window_close
            if change_pct is None or open_p is None or close_p is None:
                logger.warning(f"Памп {sig.symbol} без замеров детектора — пропущен")
                continue

            # Persist to DB
            ps_signal = PriceSurgeSignal(
                timestamp=datetime.now(tz=timezone.utc),
                symbol=sig.symbol,
                change_pct=change_pct,
                interval_minutes=interval,
            )
            session.add(ps_signal)

            # Count signals for this ticker in the last 24 hours
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
            day_count = (
                await session.scalar(
                    select(func.count())
                    .select_from(PriceSurgeSignal)
                    .where(
                        PriceSurgeSignal.symbol == sig.symbol,
                        PriceSurgeSignal.timestamp >= cutoff,
                    )
                )
                or 0
            )

            # 1-hour price change
            hour_change = await self._calc_hour_change(
                session, sig.exchange, sig.symbol, hour_bars
            )

            # OI change over last 3 points
            oi_change = await self._calc_oi_change(session, sig.symbol)

            # CoinGlass link (ByBit is the trading exchange)
            pair = sig.symbol.split("/")[0] + "USDT"
            link = f"https://www.coinglass.com/tv/Bybit_{pair}"

            text = (
                f'📈 <b><a href="{link}">{sig.symbol}</a></b> '
                f"+{change_pct:.1f}% за {interval} мин\n\n"
                f"Рост за {interval} мин: +{change_pct:.1f}%  |  "
                f"${open_p:.6f} → ${close_p:.6f}\n"
                f"Рост за 1 час: {hour_change:+.1f}%\n"
                f"Изменение OI: {oi_change:+.1f}%\n\n"
                f"Сигналов за сутки: {day_count}"
            )
            outbox.append(text)

        return outbox

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _calc_hour_change(
        self, session: AsyncSession, exchange: str | None, symbol: str, hour_bars: int,
    ) -> float:
        """Calculate price change over the last hour from OHLCV data.

        Только по бирже, на которой детектор увидел памп: без фильтра бары
        двух бирж перемешиваются, и «час» оказывается получасом."""
        hour_rows = (
            await session.execute(
                select(Candle.open, Candle.close)
                .where(Candle.exchange == exchange, Candle.symbol == symbol)
                .order_by(desc(Candle.timestamp))
                .limit(hour_bars + 1)
            )
        ).all()
        if len(hour_rows) >= hour_bars + 1:
            return (hour_rows[0][1] / hour_rows[-1][0] - 1) * 100
        return 0.0

    async def _calc_oi_change(
        self, session: AsyncSession, symbol: str,
    ) -> float:
        """Calculate OI change over the last 3 data points."""
        # exchange.in_(...) вместо запроса «по любой бирже»: две величины дают
        # два поиска по составному индексу вместо скана 9.6 млн строк.
        oi_vals = (
            await session.execute(
                select(OpenInterest.value)
                .where(
                    OpenInterest.exchange.in_(self._exchanges),
                    OpenInterest.symbol == symbol,
                )
                .order_by(desc(OpenInterest.timestamp))
                .limit(3)
            )
        ).scalars().all()
        if len(oi_vals) >= 2:
            return (oi_vals[0] / oi_vals[-1] - 1) * 100
        return 0.0
