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
from src.notifier.telegram_bot import TelegramNotifier
from src.storage.models import Candle, OpenInterest, PriceSurgeSignal

logger = logging.getLogger(__name__)


class PriceSurgeSignalProcessor:
    """Enriches price surge signals with additional metrics and sends notifications."""

    def __init__(
        self,
        config: StrategyConfig,
        detector: PriceSurgeDetector,
        notifier: TelegramNotifier,
        timeframe: str = "3m",
        data_provider: DataProvider | None = None,
    ):
        self.config = config
        self.detector = detector
        self.notifier = notifier
        self.timeframe = timeframe
        self._dp = data_provider or DataProvider()

    @property
    def data_provider(self) -> DataProvider:
        return self._dp

    @data_provider.setter
    def data_provider(self, dp: DataProvider) -> None:
        self._dp = dp
        self.detector.data_provider = dp

    async def process_and_notify(self, session: AsyncSession) -> None:
        """Run detector, enrich each signal, save to DB, and notify."""
        signals = await self.detector.analyze(session)

        if not signals:
            return

        interval = self.config.price_surge_minutes
        window_bars = self.detector._window_bars

        # Pre-compute hour_bars from timeframe
        tf_min = timeframe_to_minutes(self.timeframe)
        hour_bars = max(60 // tf_min, 1)

        for sig in signals:
            # Fetch exact prices from candles for the window (any exchange)
            c_rows = (
                await session.execute(
                    select(Candle.open, Candle.close, Candle.timestamp)
                    .where(Candle.symbol == sig.symbol)
                    .order_by(desc(Candle.timestamp))
                    .limit(window_bars + 1)
                )
            ).all()
            if len(c_rows) < window_bars + 1:
                continue
            c_rows = list(reversed(c_rows))
            open_p = c_rows[0][0]
            close_p = c_rows[-1][1]
            change_pct = (close_p / open_p - 1) * 100 if open_p > 0 else 0

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
                session, sig.symbol, hour_bars
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
            await self.notifier.notify_all(text)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _calc_hour_change(
        self, session: AsyncSession, symbol: str, hour_bars: int,
    ) -> float:
        """Calculate price change over the last hour from OHLCV data."""
        hour_rows = (
            await session.execute(
                select(Candle.open, Candle.close)
                .where(Candle.symbol == symbol)
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
        oi_vals = (
            await session.execute(
                select(OpenInterest.value)
                .where(OpenInterest.symbol == symbol)
                .order_by(desc(OpenInterest.timestamp))
                .limit(3)
            )
        ).scalars().all()
        if len(oi_vals) >= 2:
            return (oi_vals[0] / oi_vals[-1] - 1) * 100
        return 0.0
