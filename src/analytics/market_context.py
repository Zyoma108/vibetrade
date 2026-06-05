"""
Market context: OTHERS index with Supertrend (1h) + BTC 1h change.

Data sources:
- OTHERS index: TradingView (CRYPTOCAP:OTHERS) — real market cap excluding top 10
- BTC 1h change: from exchange OHLCV or ticker data

Determines the market regime (risk_on / cautious / risk_off) to:
- Block entries during risk-off
- Halve position size during cautious
- Notify on trend changes via Telegram
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import MarketContextConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Ticker

logger = logging.getLogger(__name__)

# TradingView symbols
_TV_OTHERS_SYMBOL = "CRYPTOCAP:OTHERS"
_TV_OTHERS_EXCHANGE = "CRYPTOCAP"
# Number of 1h bars to fetch
_BARS_TO_FETCH = 30
# Keep this many bars in memory
_PROXY_HISTORY_BARS = 25
# Update interval
_UPDATE_INTERVAL = timedelta(minutes=30)


class MarketContext:
    """Evaluates market conditions using TradingView OTHERS + exchange BTC data."""

    def __init__(self, config: MarketContextConfig, connector: ExchangeConnector):
        self.config = config
        self._connector = connector
        self._enabled = config.enabled
        self._tv = None  # Lazy init — TvDatafeed может падать при импорте

        # OTHERS OHLCV history (list of dicts, chronological)
        self._bars: list[dict] = []
        self._last_update: datetime | None = None

        # Current state
        self._regime: str = "unknown"
        self._regime_start: datetime = datetime.now(tz=timezone.utc)
        self._supertrend_color: str = "red"
        self._btc_change_1h: float = 0.0
        self._btc_change_4h: float = 0.0
        self._others_value: float = 0.0
        self._others_change_1h: float = 0.0
        self._others_change_4h: float = 0.0

        # Trend (bullish/bearish/neutral) — for TP/trailing stop decisions
        self._trend: str = "neutral"
        self._trend_start: datetime = datetime.now(tz=timezone.utc)

        # Previous trend for notifications and /trend
        self._prev_regime: str | None = None
        self._prev_regime_start: datetime | None = None
        self._prev_regime_end: datetime | None = None
        self._prev_trend: str | None = None

        self._changed = False
        self._trend_changed_flag: bool = False
        self._ready = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def regime(self) -> str:
        return self._regime

    @property
    def supertrend_color(self) -> str:
        return self._supertrend_color

    @property
    def btc_change_1h(self) -> float:
        return self._btc_change_1h

    @property
    def btc_change_4h(self) -> float:
        return self._btc_change_4h

    @property
    def others_change_4h(self) -> float:
        return self._others_change_4h

    @property
    def trend(self) -> str:
        """Current trend: bullish, bearish, or neutral.

        Used for TP/trailing stop decisions.
        """
        if not self._ready:
            return "neutral"
        return self._trend

    @property
    def prev_trend(self) -> str | None:
        """Previous trend before the last change, for notifications."""
        return self._prev_trend

    @property
    def trend_changed(self) -> bool:
        """One-shot flag: returns True once per trend change, then resets."""
        if self._trend_changed_flag:
            self._trend_changed_flag = False
            return True
        return False

    def should_block_entries(self) -> bool:
        if not self._enabled or not self._ready:
            return False
        return self._regime == "risk_off"

    def position_size_multiplier(self) -> float:
        if not self._enabled or not self._ready:
            return 1.0
        if self._regime == "risk_off":
            return 0.0
        if self._regime == "cautious":
            return 0.5
        return 1.0

    @property
    def regime_changed(self) -> bool:
        """One-shot flag: returns True once per regime change, then resets."""
        if self._changed:
            self._changed = False
            return True
        return False

    def trend_summary(self) -> str:
        """Formatted trend info for /trend command."""
        if not self._ready:
            return (
                "📊 <b>Рыночный контекст</b>\n\n"
                "Ещё собираю данные…\n"
                f"Обновление раз в {_UPDATE_INTERVAL.seconds // 60} мин."
            )

        btcs_1h = (
            f"🔻 {self._btc_change_1h:+.1f}%"
            if self._btc_change_1h < 0
            else f"🟢 +{self._btc_change_1h:.1f}%"
        )
        btcs_4h = (
            f"🔻 {self._btc_change_4h:+.1f}%"
            if self._btc_change_4h < 0
            else f"🟢 +{self._btc_change_4h:.1f}%"
        )
        others_1h = (
            f"🔻 {self._others_change_1h:+.1f}%"
            if self._others_change_1h < 0
            else f"🟢 +{self._others_change_1h:.1f}%"
        )
        others_4h = (
            f"🔻 {self._others_change_4h:+.1f}%"
            if self._others_change_4h < 0
            else f"🟢 +{self._others_change_4h:.1f}%"
        )
        st_emoji = "🟢" if self._supertrend_color == "green" else "🔴"
        regime_emoji = {
            "risk_on": "🟢 RISK-ON",
            "cautious": "🟡 CAUTIOUS",
            "risk_off": "🔴 RISK-OFF",
        }.get(self._regime, "⚪ UNKNOWN")
        trend_emoji = {
            "bullish": "🟢 BULLISH",
            "bearish": "🔴 BEARISH",
            "neutral": "⚪ NEUTRAL",
        }.get(self._trend, "⚪ NEUTRAL")

        regime_duration = self._format_duration(
            (datetime.now(tz=timezone.utc) - self._regime_start).total_seconds()
        )

        lines = [
            "📊 <b>Рыночный контекст</b>\n",
            f"Режим: <b>{regime_emoji}</b> (⏱ {regime_duration})",
            f"Тренд: <b>{trend_emoji}</b>",
            "",
            f"{st_emoji} <b>OTHERS Supertrend</b> (1h, "
            f"{self.config.supertrend_atr_period},{self.config.supertrend_multiplier})",
            f"OTHERS: ${self._others_value / 1e9:.2f}B | 1h: {others_1h} | 4h: {others_4h}",
            f"BTC 1h: {btcs_1h} | 4h: {btcs_4h}",
        ]

        if self._prev_regime and self._prev_regime_start and self._prev_regime_end:
            prev_dur = (
                self._prev_regime_end - self._prev_regime_start
            ).total_seconds()
            prev_emoji = {
                "risk_on": "🟢 RISK-ON",
                "cautious": "🟡 CAUTIOUS",
                "risk_off": "🔴 RISK-OFF",
            }.get(self._prev_regime, self._prev_regime)
            lines.append(
                f"\nПредыдущий: {prev_emoji} ({self._format_duration(prev_dur)})"
            )

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Update cycle
    # ------------------------------------------------------------------

    async def update(self, session: AsyncSession, force: bool = False) -> str:
        """Run market context update (throttled to every 30 min)."""
        if not self._enabled:
            self._regime = "unknown"
            return self._regime

        now = datetime.now(tz=timezone.utc)
        if not force and self._last_update and now - self._last_update < _UPDATE_INTERVAL:
            return self._regime

        self._last_update = now

        try:
            # 1. Fetch OTHERS 1h from TradingView (or fallback to exchange)
            await self._fetch_others_data()

            # 2. Compute Supertrend on OTHERS bars
            self._compute_supertrend()

            # 3. BTC 1h and 4h changes from exchange
            self._btc_change_1h, self._btc_change_4h = \
                await self._calc_btc_changes(session)

            # 4. Determine regime (entry gating)
            new_regime = self._determine_regime()

            # 5. Determine trend (for TP/trailing stop decisions)
            new_trend = self._determine_trend()

            # 6. Track changes
            self._changed = False
            if new_regime != self._regime and self._regime != "unknown":
                self._changed = True
                self._prev_regime = self._regime
                self._prev_regime_start = self._regime_start
                self._prev_regime_end = now
                logger.info(
                    f"Режим сменился: {self._regime} → {new_regime} "
                    f"(BTC={self._btc_change_1h:+.1f}%, ST={self._supertrend_color})"
                )

            self._regime = new_regime
            if self._changed or self._regime_start is None:
                self._regime_start = now

            # Track trend changes
            self._trend_changed_flag = False
            if new_trend != self._trend:
                old_trend = self._trend
                self._prev_trend = old_trend
                # Suppress only the very first (startup) determination
                if self._ready:
                    self._trend_changed_flag = True
                logger.info(
                    f"Тренд сменился: {old_trend} → {new_trend} "
                    f"(OTHERS_4h={self._others_change_4h:+.1f}%, "
                    f"BTC_4h={self._btc_change_4h:+.1f}%, "
                    f"ST={self._supertrend_color})"
                )

            self._trend = new_trend
            if self._trend_changed_flag or self._trend == "neutral":
                self._trend_start = now

            self._ready = True
            logger.info(
                f"MarketContext: regime={self._regime} trend={self._trend} "
                f"BTC_1h={self._btc_change_1h:+.1f}% BTC_4h={self._btc_change_4h:+.1f}% "
                f"ST={self._supertrend_color} "
                f"OTHERS=${self._others_value / 1e9:.2f}B "
                f"OTHERS_4h={self._others_change_4h:+.1f}% "
                f"bars={len(self._bars)}"
            )

        except Exception:
            logger.exception("Ошибка обновления MarketContext")
            if not self._ready:
                self._regime = "unknown"

        return self._regime

    # ------------------------------------------------------------------
    # OTHERS data (TradingView primary, exchange fallback)
    # ------------------------------------------------------------------

    async def _fetch_others_data(self) -> None:
        """Fetch OTHERS 1h candles. Primary: TradingView. Fallback: exchange proxy."""
        df = await self._fetch_tv_others()
        if df is not None and len(df) >= 2:
            self._bars = self._df_to_bars(df)
            self._update_others_metrics()
            return

        logger.warning("MarketContext: TradingView недоступен, пробую прокси через биржу")
        logger.warning("MarketContext: прокси через биржу пока не реализован")

    async def _fetch_tv_others(self) -> pd.DataFrame | None:
        """Fetch OTHERS 1h from TradingView using tvdatafeed."""
        try:
            if self._tv is None:
                from tvDatafeed import TvDatafeed, Interval
                self._tv = TvDatafeed()
                self._tv_interval = Interval.in_1_hour

            df = await asyncio.to_thread(
                self._tv.get_hist,
                symbol=_TV_OTHERS_SYMBOL,
                exchange=_TV_OTHERS_EXCHANGE,
                interval=self._tv_interval,
                n_bars=_BARS_TO_FETCH,
            )
            if df is None or df.empty:
                return None
            return df
        except Exception as e:
            logger.warning(f"MarketContext: TradingView error: {e}")
            return None

    def _df_to_bars(self, df: pd.DataFrame) -> list[dict]:
        """Convert TradingView DataFrame to our bars format (chronological)."""
        bars = []
        for idx, row in df.iterrows():
            bars.append({
                "timestamp": idx,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
        # TradingView DF is already chronological (oldest first)
        if len(bars) > _PROXY_HISTORY_BARS:
            bars = bars[-_PROXY_HISTORY_BARS:]
        return bars

    def _update_others_metrics(self) -> None:
        """Update OTHERS value, 1h and 4h changes from bars."""
        if not self._bars:
            return
        latest = self._bars[-1]
        self._others_value = latest["close"]
        # 1h change
        if len(self._bars) >= 2:
            prev_close = self._bars[-2]["close"]
            if prev_close > 0:
                self._others_change_1h = (
                    (self._others_value / prev_close - 1) * 100
                )
        # 4h change
        if len(self._bars) >= 5:
            prev_4h_close = self._bars[-5]["close"]
            if prev_4h_close > 0:
                self._others_change_4h = (
                    (self._others_value / prev_4h_close - 1) * 100
                )

    # ------------------------------------------------------------------
    # Supertrend
    # ------------------------------------------------------------------

    def _compute_supertrend(self) -> None:
        """Compute Supertrend on the OTHERS 1h bars."""
        period = self.config.supertrend_atr_period
        mult = self.config.supertrend_multiplier
        min_bars = period + 1

        if len(self._bars) < min_bars:
            logger.debug(
                f"MarketContext: недостаточно баров для Supertrend "
                f"({len(self._bars)} < {min_bars})"
            )
            return

        highs = np.array([b["high"] for b in self._bars])
        lows = np.array([b["low"] for b in self._bars])
        closes = np.array([b["close"] for b in self._bars])

        # ATR (Wilder's smoothing)
        tr = np.maximum(
            highs - lows,
            np.maximum(
                np.abs(highs - np.roll(closes, 1)),
                np.abs(lows - np.roll(closes, 1)),
            ),
        )
        tr[0] = highs[0] - lows[0]
        atr = np.zeros(len(tr))
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, len(tr)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        # Supertrend bands
        hl2 = (highs + lows) / 2
        upper = hl2 + mult * atr
        lower = hl2 - mult * atr

        # Final bands with carry-forward logic
        final_upper = np.zeros(len(upper))
        final_lower = np.zeros(len(lower))
        trend = np.zeros(len(closes), dtype=int)  # 1 = green, -1 = red

        for i in range(period, len(closes)):
            if upper[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]:
                final_upper[i] = upper[i]
            else:
                final_upper[i] = final_upper[i - 1]

            if lower[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]:
                final_lower[i] = lower[i]
            else:
                final_lower[i] = final_lower[i - 1]

            if closes[i] > final_upper[i - 1]:
                trend[i] = 1
            elif closes[i] < final_lower[i - 1]:
                trend[i] = -1
            else:
                trend[i] = trend[i - 1] if i > period else -1

        if len(trend) > period:
            self._supertrend_color = "green" if trend[-1] == 1 else "red"

    # ------------------------------------------------------------------
    # BTC
    # ------------------------------------------------------------------

    async def _calc_btc_changes(self, session: AsyncSession) -> tuple[float, float]:
        """Calculate BTC/USDT change over 1h and 4h.

        Uses exchange OHLCV (5 bars of 1h) or ticker data as fallback.
        Returns (change_1h, change_4h).
        """
        change_1h = 0.0
        change_4h = 0.0

        # Try 1h candles from exchange (fetch 5 to cover 4h)
        try:
            btc_candles = await self._connector.fetch_ohlcv(
                "BTC/USDT", "1h", limit=5
            )
            if btc_candles and len(btc_candles) >= 2:
                current = btc_candles[-1]["close"]
                # 1h change
                prev_1h = btc_candles[-2]["close"]
                if prev_1h > 0:
                    change_1h = (current / prev_1h - 1) * 100
                # 4h change
                if len(btc_candles) >= 5:
                    prev_4h = btc_candles[-5]["close"]
                    if prev_4h > 0:
                        change_4h = (current / prev_4h - 1) * 100
                return change_1h, change_4h
        except Exception:
            pass

        # Fallback: use ticker prices from DB (1h only; 4h from tickers is unreliable)
        ticker_rows = (
            await session.execute(
                select(Ticker.last, Ticker.timestamp)
                .where(Ticker.symbol.in_(["BTC/USDT", "BTC/USDT:USDT"]))
                .order_by(desc(Ticker.timestamp))
                .limit(200)
            )
        ).all()
        if len(ticker_rows) >= 2:
            current = ticker_rows[0][0]
            now = datetime.now(tz=timezone.utc)

            # 1h change from tickers
            cutoff_1h = now - timedelta(hours=1)
            for price, ts in ticker_rows:
                if ts and ts <= cutoff_1h and ts > now - timedelta(hours=2):
                    if price > 0 and current > 0:
                        change_1h = (current / price - 1) * 100
                        break
            if change_1h == 0.0 and len(ticker_rows) > 10:
                best = ticker_rows[-1][0]
                if best > 0 and current > 0:
                    change_1h = (current / best - 1) * 100

            # 4h change from tickers
            cutoff_4h = now - timedelta(hours=4)
            for price, ts in ticker_rows:
                if ts and ts <= cutoff_4h and ts > now - timedelta(hours=5):
                    if price > 0 and current > 0:
                        change_4h = (current / price - 1) * 100
                        break

        return change_1h, change_4h

    # ------------------------------------------------------------------
    # Regime logic
    # ------------------------------------------------------------------

    def _determine_regime(self) -> str:
        """Combine BTC 1h change and Supertrend to determine regime."""
        btc_bearish = self._btc_change_1h < -self.config.btc_drop_threshold_pct
        st_bullish = self._supertrend_color == "green"

        if btc_bearish and not st_bullish:
            return "risk_off"
        elif btc_bearish or not st_bullish:
            return "cautious"
        else:
            return "risk_on"

    def _determine_trend(self) -> str:
        """Determine trend based on OTHERS Supertrend + 4h OTHERS/BTC changes.

        Bullish: OTHERS ST green + OTHERS 4h >= threshold + BTC 4h >= threshold
        Bearish: OTHERS ST red + OTHERS 4h <= -threshold + BTC 4h <= -threshold
        Neutral: everything else.
        """
        threshold = self.config.trend_threshold_pct
        st_bullish = self._supertrend_color == "green"

        others_up = self._others_change_4h >= threshold
        others_down = self._others_change_4h <= -threshold
        btc_up = self._btc_change_4h >= threshold
        btc_down = self._btc_change_4h <= -threshold

        if st_bullish and others_up and btc_up:
            return "bullish"
        elif not st_bullish and others_down and btc_down:
            return "bearish"
        else:
            return "neutral"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_duration(total_seconds: float) -> str:
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        if hours >= 24:
            days = hours // 24
            return f"{days}д {hours % 24}ч"
        if hours > 0:
            return f"{hours}ч {minutes}м"
        return f"{minutes}м"
