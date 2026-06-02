"""
Market context: OTHERS proxy with Supertrend (1h) + BTC 1h change.

Determines the market regime (risk_on / cautious / risk_off) to:
- Block entries during risk-off
- Halve position size during cautious
- Notify on trend changes via Telegram

Data sources (no external APIs):
- OTHERS proxy: average price of top-N altcoins from our own candle DB
- BTC 1h change: from BTC/USDT ticker and 1h-ago candle
- Supertrend on 1h OTHERS proxy candles
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import MarketContextConfig
from src.storage.models import Candle, Ticker

logger = logging.getLogger(__name__)

# Supertrend periods to keep in memory (15 × 1h = enough for ATR + buffer)
_PROXY_HISTORY_BARS = 20

# Exclude top-5 coins from OTHERS proxy calculation
_TOP5_EXCLUDE = {"BTC", "ETH", "BNB", "SOL", "XRP"}

# Supertrend 1h candles needed for initial populate
_INITIAL_1H_BARS = 15
# 3m candles per 1h candle
_BARS_PER_HOUR = 20  # 60 / 3 = 20


@dataclass
class TrendInfo:
    """Snapshot of the current/previous trend for notifications and /trend."""
    regime: str  # "risk_on" / "cautious" / "risk_off"
    supertrend: str  # "🟢 Uptrend" / "🔴 Downtrend"
    btc_change_1h: float  # %
    others_change_1h: float  # %
    since: datetime
    prev_regime: str | None = None
    prev_duration_hours: float | None = None


class MarketContext:
    """Evaluates market conditions and determines the trading regime."""

    def __init__(self, config: MarketContextConfig):
        self.config = config
        self._enabled = config.enabled

        # OTHERS proxy OHLCV history (list of dicts, chronological)
        self._proxy_bars: list[dict] = []
        # Last timestamp we computed a 1h bar for
        self._last_1h_ts: datetime | None = None

        # Current state
        self._regime: str = "unknown"
        self._regime_start: datetime = datetime.now(tz=timezone.utc)
        self._supertrend_color: str = "red"
        self._supertrend_value: float = 0.0
        self._btc_change_1h: float = 0.0
        self._others_value: float = 0.0
        self._others_change_1h: float = 0.0

        # Previous trend for notifications
        self._prev_regime: str | None = None
        self._prev_regime_start: datetime | None = None
        self._prev_regime_end: datetime | None = None

        # Flag: first successful update completed
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

    def should_block_entries(self) -> bool:
        """Return True if new positions should NOT be opened."""
        if not self._enabled or not self._ready:
            return False
        return self._regime == "risk_off"

    def position_size_multiplier(self) -> float:
        """Multiplier for position size based on regime."""
        if not self._enabled or not self._ready:
            return 1.0
        if self._regime == "risk_off":
            return 0.0
        if self._regime == "cautious":
            return 0.5
        return 1.0  # risk_on + unknown

    def trend_summary(self) -> str:
        """Formatted trend info for /trend command."""
        if not self._ready:
            return "📊 <b>Рыночный контекст</b>\n\nЕщё собираю данные..."

        btcs = (
            f"🔻 {self._btc_change_1h:+.1f}%"
            if self._btc_change_1h < 0
            else f"🟢 +{self._btc_change_1h:.1f}%"
        )
        others_s = (
            f"🔻 {self._others_change_1h:+.1f}%"
            if self._others_change_1h < 0
            else f"🟢 +{self._others_change_1h:.1f}%"
        )
        st_emoji = "🟢" if self._supertrend_color == "green" else "🔴"
        regime_emoji = {
            "risk_on": "🟢 RISK-ON",
            "cautious": "🟡 CAUTIOUS",
            "risk_off": "🔴 RISK-OFF",
        }.get(self._regime, "⚪ UNKNOWN")

        duration = self._format_duration(
            (datetime.now(tz=timezone.utc) - self._regime_start).total_seconds()
        )

        lines = [
            "📊 <b>Рыночный контекст</b>\n",
            f"Режим: <b>{regime_emoji}</b> (⏱ {duration})",
            "",
            f"{st_emoji} <b>OTHERS Supertrend</b> (1h, {self.config.supertrend_atr_period},{self.config.supertrend_multiplier})",
            f"Индекс OTHERS: {self._others_value:.2f} | 1h: {others_s}",
            f"BTC 1h: {btcs}",
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

    def check_trend_change(self) -> TrendInfo | None:
        """Return TrendInfo if regime just changed, None otherwise.
        Called after update() — if returns non-None, it's a fresh change
        and the caller should notify.
        """
        # This is consumed once per change
        return None  # We handle notification in the Application layer

    @property
    def regime_changed(self) -> bool:
        """True if regime changed in the last update (read-once flag)."""
        return getattr(self, "_changed", False)

    # ------------------------------------------------------------------
    # Update cycle
    # ------------------------------------------------------------------

    async def update(self, session: AsyncSession) -> str:
        """Run a full market context update. Returns current regime."""
        if not self._enabled:
            self._regime = "unknown"
            return self._regime

        try:
            # 1. Compute OTHERS proxy 1h candles
            await self._update_others_proxy(session)

            # 2. Compute Supertrend on proxy
            self._compute_supertrend()

            # 3. BTC 1h change
            self._btc_change_1h = await self._calc_btc_change(session)

            # 4. Determine regime
            new_regime = self._determine_regime()

            # 5. Track changes
            self._changed = False
            if new_regime != self._regime and self._regime != "unknown":
                self._changed = True
                self._prev_regime = self._regime
                self._prev_regime_start = self._regime_start
                self._prev_regime_end = datetime.now(tz=timezone.utc)
                logger.info(
                    f"Режим сменился: {self._regime} → {new_regime} "
                    f"(BTC={self._btc_change_1h:+.1f}%, ST={self._supertrend_color})"
                )

            self._regime = new_regime
            if self._changed or self._regime_start is None:
                self._regime_start = datetime.now(tz=timezone.utc)

            self._ready = True

        except Exception:
            logger.exception("Ошибка обновления MarketContext")
            if not self._ready:
                self._regime = "unknown"

        return self._regime

    # ------------------------------------------------------------------
    # OTHERS proxy
    # ------------------------------------------------------------------

    async def _update_others_proxy(self, session: AsyncSession) -> None:
        """Build or update OTHERS proxy 1h candles from top altcoin 3m candles."""
        # Get top altcoins by volume
        top_alts = await self._get_top_altcoins(session)
        if not top_alts:
            return

        # Determine how many 1h bars to compute
        if not self._proxy_bars:
            need_bars = _INITIAL_1H_BARS  # initial load
        else:
            # Only compute new bars since last update
            need_bars = 2  # latest + possibly one more

        # Load 3m candles for each altcoin
        three_m_limit = (need_bars + 2) * _BARS_PER_HOUR  # extra buffer
        alt_data: dict[str, pd.DataFrame] = {}
        for symbol in top_alts:
            df = await self._load_candles_df(session, symbol, three_m_limit)
            if df is not None and len(df) >= _BARS_PER_HOUR:
                alt_data[symbol] = df

        if not alt_data:
            return

        # Resample each to 1h and compute average close
        proxy_1h = self._build_proxy_1h(alt_data, need_bars)

        if proxy_1h:
            if not self._proxy_bars:
                self._proxy_bars = proxy_1h
            else:
                # Merge: replace last few bars with new data
                new_timestamps = {b["timestamp"] for b in proxy_1h}
                self._proxy_bars = [
                    b for b in self._proxy_bars
                    if b["timestamp"] not in new_timestamps
                ]
                self._proxy_bars.extend(proxy_1h)
                self._proxy_bars.sort(key=lambda b: b["timestamp"])
                # Keep only the last N
                if len(self._proxy_bars) > _PROXY_HISTORY_BARS:
                    self._proxy_bars = self._proxy_bars[-_PROXY_HISTORY_BARS:]

            # Current OTHERS value
            if self._proxy_bars:
                latest = self._proxy_bars[-1]
                self._others_value = latest["close"]
                if len(self._proxy_bars) >= 2:
                    prev_close = self._proxy_bars[-2]["close"]
                    if prev_close > 0:
                        self._others_change_1h = (
                            (self._others_value / prev_close - 1) * 100
                        )

    async def _get_top_altcoins(self, session: AsyncSession) -> list[str]:
        """Get top-N altcoins by volume, excluding top-5 and our exclusions."""
        result = await session.execute(
            select(Ticker.symbol, Ticker.volume)
            .where(Ticker.exchange == "bybit")
            .order_by(desc(Ticker.volume))
            .limit(200)  # fetch more than needed, filter in Python
        )
        rows = result.all()
        alts = []
        for symbol, volume in rows:
            if not symbol.endswith("/USDT"):
                continue
            base = symbol.split("/")[0].upper()
            if base in _TOP5_EXCLUDE:
                continue
            if volume and volume > 0:
                alts.append(symbol)
            if len(alts) >= self.config.altcoin_sample_size:
                break
        return alts

    async def _load_candles_df(
        self, session: AsyncSession, symbol: str, limit: int
    ) -> pd.DataFrame | None:
        """Load 3m candles as a pandas DataFrame."""
        result = await session.execute(
            select(
                Candle.timestamp, Candle.open, Candle.high,
                Candle.low, Candle.close, Candle.volume,
            )
            .where(Candle.symbol == symbol)
            .order_by(desc(Candle.timestamp))
            .limit(limit)
        )
        rows = result.all()
        if len(rows) < _BARS_PER_HOUR:
            return None

        df = pd.DataFrame(
            rows,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp").sort_index()
        return df

    def _build_proxy_1h(
        self, alt_data: dict[str, pd.DataFrame], need_bars: int
    ) -> list[dict]:
        """Build OTHERS proxy 1h bars from multiple altcoin 3m DataFrames."""
        # Resample each altcoin to 1h
        hourly_closes: dict[pd.Timestamp, list[float]] = {}
        hourly_ohcl: dict[pd.Timestamp, list[dict]] = {}

        for symbol, df in alt_data.items():
            ohlc = df.resample("1h").agg({
                "open": "first", "high": "max",
                "low": "min", "close": "last",
                "volume": "sum",
            }).dropna()

            for ts, row in ohlc.iterrows():
                if row["close"] > 0:
                    hourly_closes.setdefault(ts, []).append(row["close"])
                    hourly_ohcl.setdefault(ts, []).append({
                        "open": row["open"], "high": row["high"],
                        "low": row["low"], "close": row["close"],
                    })

        if not hourly_closes:
            return []

        # For each 1h timestamp, compute average OHLCV across altcoins
        result = []
        for ts in sorted(hourly_closes.keys()):
            closes = hourly_closes[ts]
            ohcl = hourly_ohcl[ts]
            avg_close = sum(closes) / len(closes)
            avg_open = sum(b["open"] for b in ohcl) / len(ohcl)
            avg_high = sum(b["high"] for b in ohcl) / len(ohcl)
            avg_low = sum(b["low"] for b in ohcl) / len(ohcl)

            result.append({
                "timestamp": ts,
                "open": avg_open,
                "high": avg_high,
                "low": avg_low,
                "close": avg_close,
            })

        return result[-need_bars:] if len(result) > need_bars else result

    # ------------------------------------------------------------------
    # Supertrend
    # ------------------------------------------------------------------

    def _compute_supertrend(self) -> None:
        """Compute Supertrend on the OTHERS proxy 1h bars."""
        if len(self._proxy_bars) < self.config.supertrend_atr_period + 1:
            return

        period = self.config.supertrend_atr_period
        mult = self.config.supertrend_multiplier

        highs = np.array([b["high"] for b in self._proxy_bars])
        lows = np.array([b["low"] for b in self._proxy_bars])
        closes = np.array([b["close"] for b in self._proxy_bars])

        # ATR
        tr = np.maximum(
            highs - lows,
            np.maximum(
                np.abs(highs - np.roll(closes, 1)),
                np.abs(lows - np.roll(closes, 1)),
            ),
        )
        tr[0] = highs[0] - lows[0]
        atr = np.zeros_like(tr)
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, len(tr)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        # Supertrend bands
        hl2 = (highs + lows) / 2
        upper = hl2 + mult * atr
        lower = hl2 - mult * atr

        # Final bands
        final_upper = np.zeros_like(upper)
        final_lower = np.zeros_like(lower)
        trend = np.zeros(len(closes), dtype=int)  # 1 = green, -1 = red

        for i in range(period, len(closes)):
            # Upper band
            if upper[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]:
                final_upper[i] = upper[i]
            else:
                final_upper[i] = final_upper[i - 1]

            # Lower band
            if lower[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]:
                final_lower[i] = lower[i]
            else:
                final_lower[i] = final_lower[i - 1]

            # Trend direction
            if closes[i] > final_upper[i - 1]:
                trend[i] = 1
            elif closes[i] < final_lower[i - 1]:
                trend[i] = -1
            else:
                trend[i] = trend[i - 1] if i > period else -1

        if len(trend) > period:
            self._supertrend_color = "green" if trend[-1] == 1 else "red"
            self._supertrend_value = (
                final_lower[-1] if trend[-1] == 1 else final_upper[-1]
            )

    # ------------------------------------------------------------------
    # BTC
    # ------------------------------------------------------------------

    async def _calc_btc_change(self, session: AsyncSession) -> float:
        """Calculate BTC/USDT change over the last hour."""
        result = await session.execute(
            select(Candle.close)
            .where(Candle.symbol == "BTC/USDT")
            .order_by(desc(Candle.timestamp))
            .limit(_BARS_PER_HOUR + 1)
        )
        rows = result.all()
        if len(rows) >= _BARS_PER_HOUR + 1:
            current = rows[0][0]
            hour_ago = rows[-1][0]
            if hour_ago > 0:
                return (current / hour_ago - 1) * 100
        return 0.0

    # ------------------------------------------------------------------
    # Regime logic
    # ------------------------------------------------------------------

    def _determine_regime(self) -> str:
        """Combine BTC 1h change and Supertrend to determine regime."""
        btc_bearish = self._btc_change_1h < -self.config.btc_drop_threshold_pct
        st_bullish = self._supertrend_color == "green"

        if btc_bearish and not st_bullish:
            return "risk_off"  # BTC падает + альты в даунтренде
        elif btc_bearish or not st_bullish:
            return "cautious"  # один из сигналов негативный
        else:
            return "risk_on"  # BTC стабилен + альты в аптренде

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
