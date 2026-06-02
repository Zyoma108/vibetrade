"""
Market context: OTHERS proxy with Supertrend (1h) + BTC 1h change.

Fetches 1h candles directly from exchange every 30 minutes — no DB storage,
no impact on the main 3m collector. Everything is kept in memory.

Determines the market regime (risk_on / cautious / risk_off) to:
- Block entries during risk-off
- Halve position size during cautious
- Notify on trend changes via Telegram
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import MarketContextConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Ticker

logger = logging.getLogger(__name__)

# Number of 1h bars to fetch per symbol
_BARS_TO_FETCH = 30
# Keep this many proxy bars in memory
_PROXY_HISTORY_BARS = 25
# Exclude from OTHERS proxy (top market cap)
_TOP5_EXCLUDE = {"BTC", "ETH", "BNB", "SOL", "XRP"}
# Update interval
_UPDATE_INTERVAL = timedelta(minutes=30)


class MarketContext:
    """Evaluates market conditions using 1h candles from the exchange."""

    def __init__(self, config: MarketContextConfig, connector: ExchangeConnector):
        self.config = config
        self._connector = connector
        self._enabled = config.enabled

        # OTHERS proxy OHLCV history (list of dicts, chronological)
        self._proxy_bars: list[dict] = []
        self._last_update: datetime | None = None

        # Current state
        self._regime: str = "unknown"
        self._regime_start: datetime = datetime.now(tz=timezone.utc)
        self._supertrend_color: str = "red"
        self._btc_change_1h: float = 0.0
        self._others_value: float = 0.0
        self._others_change_1h: float = 0.0

        # Previous trend for notifications and /trend
        self._prev_regime: str | None = None
        self._prev_regime_start: datetime | None = None
        self._prev_regime_end: datetime | None = None

        self._changed = False
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
        return self._changed

    def trend_summary(self) -> str:
        """Formatted trend info for /trend command."""
        if not self._ready:
            return (
                "📊 <b>Рыночный контекст</b>\n\n"
                "Ещё собираю данные…\n"
                f"Обновление раз в {_UPDATE_INTERVAL.seconds // 60} мин."
            )

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
            f"{st_emoji} <b>OTHERS Supertrend</b> (1h, "
            f"{self.config.supertrend_atr_period},{self.config.supertrend_multiplier})",
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

    # ------------------------------------------------------------------
    # Update cycle
    # ------------------------------------------------------------------

    async def update(self, session: AsyncSession, force: bool = False) -> str:
        """Run market context update (throttled to every 30 min).

        Set force=True to bypass the throttle (e.g. on startup).
        """
        if not self._enabled:
            self._regime = "unknown"
            return self._regime

        # Throttle
        now = datetime.now(tz=timezone.utc)
        if not force and self._last_update and now - self._last_update < _UPDATE_INTERVAL:
            return self._regime

        self._last_update = now

        try:
            # 1. Get top altcoins from tickers (already in DB from collector)
            top_alts = await self._get_top_altcoins(session)

            # 2. Fetch 1h candles from exchange for BTC + top alts
            await self._fetch_market_data(top_alts)

            # 3. Compute Supertrend on proxy
            self._compute_supertrend()

            # 4. Determine regime
            new_regime = self._determine_regime()

            # 5. Track changes
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

            self._ready = True
            logger.info(
                f"MarketContext: regime={self._regime} "
                f"BTC_1h={self._btc_change_1h:+.1f}% "
                f"ST={self._supertrend_color} "
                f"OTHERS={self._others_value:.2f} "
                f"bars={len(self._proxy_bars)}"
            )

        except Exception:
            logger.exception("Ошибка обновления MarketContext")
            if not self._ready:
                self._regime = "unknown"

        return self._regime

    # ------------------------------------------------------------------
    # Data fetching from exchange
    # ------------------------------------------------------------------

    async def _get_top_altcoins(self, session: AsyncSession) -> list[str]:
        """Get top-N altcoins by volume from the latest tickers."""
        result = await session.execute(
            select(Ticker.symbol, Ticker.volume)
            .where(Ticker.exchange == "bybit")
            .order_by(desc(Ticker.volume))
            .limit(200)
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

    async def _fetch_market_data(self, top_alts: list[str]) -> None:
        """Fetch 1h candles for BTC and top altcoins, build OTHERS proxy."""
        # Fetch BTC 1h candles
        btc_candles = await self._fetch_1h_candles("BTC/USDT")
        if btc_candles and len(btc_candles) >= 2:
            current = btc_candles[-1]["close"]
            prev = btc_candles[-2]["close"]
            if prev > 0:
                self._btc_change_1h = (current / prev - 1) * 100

        # Fetch 1h candles for top altcoins (concurrent, limited by connector semaphore)
        tasks = [self._fetch_1h_candles(s) for s in top_alts]
        results = await asyncio.gather(*tasks)
        all_1h: dict[str, list[dict]] = {}
        for symbol, candles in zip(top_alts, results):
            if candles is not None:
                all_1h[symbol] = candles

        if not all_1h:
            logger.warning("MarketContext: не удалось получить 1h свечи для альтов")
            return

        # Build OTHERS proxy: average OHLCV across all altcoins for each timestamp
        proxy_bars = self._build_proxy(all_1h)
        logger.info(
            f"MarketContext: proxy из {len(all_1h)} альтов, "
            f"{len(proxy_bars)} баров"
        )

        if proxy_bars:
            self._proxy_bars = proxy_bars
            latest = self._proxy_bars[-1]
            self._others_value = latest["close"]
            if len(self._proxy_bars) >= 2:
                prev_close = self._proxy_bars[-2]["close"]
                if prev_close > 0:
                    self._others_change_1h = (
                        (self._others_value / prev_close - 1) * 100
                    )

    async def _fetch_1h_candles(self, symbol: str) -> list[dict] | None:
        """Fetch 1h OHLCV candles from the exchange. Returns chronological list."""
        try:
            raw = await self._connector.fetch_ohlcv(symbol, "1h", limit=_BARS_TO_FETCH)
            if not raw:
                return None
            if len(raw) < 2:
                logger.debug(f"MarketContext: {symbol} — только {len(raw)} свечей 1h")
                return None
            return raw  # Already chronological from connector
        except Exception as e:
            logger.warning(f"MarketContext: {symbol} — ошибка 1h свечей: {e}")
            return None

    # ------------------------------------------------------------------
    # Proxy construction
    # ------------------------------------------------------------------

    def _build_proxy(self, all_1h: dict[str, list[dict]]) -> list[dict]:
        """Build OTHERS proxy: average of all altcoin 1h candles by timestamp."""
        # Collect closes (and OHLC) per timestamp
        by_ts: dict[int, list[dict]] = {}  # timestamp_ms → list of candle dicts

        for symbol, candles in all_1h.items():
            for c in candles:
                ts = int(c["timestamp"].timestamp() * 1000)
                by_ts.setdefault(ts, []).append(c)

        if not by_ts:
            return []

        result = []
        for ts in sorted(by_ts.keys()):
            bars = by_ts[ts]
            n = len(bars)
            result.append({
                "timestamp": bars[0]["timestamp"],
                "open": sum(b["open"] for b in bars) / n,
                "high": sum(b["high"] for b in bars) / n,
                "low": sum(b["low"] for b in bars) / n,
                "close": sum(b["close"] for b in bars) / n,
            })

        # Keep last N bars
        if len(result) > _PROXY_HISTORY_BARS:
            result = result[-_PROXY_HISTORY_BARS:]

        return result

    # ------------------------------------------------------------------
    # Supertrend
    # ------------------------------------------------------------------

    def _compute_supertrend(self) -> None:
        """Compute Supertrend on the OTHERS proxy 1h bars."""
        period = self.config.supertrend_atr_period
        mult = self.config.supertrend_multiplier
        min_bars = period + 1

        if len(self._proxy_bars) < min_bars:
            logger.debug(
                f"MarketContext: недостаточно баров для Supertrend "
                f"({len(self._proxy_bars)} < {min_bars})"
            )
            return

        highs = np.array([b["high"] for b in self._proxy_bars])
        lows = np.array([b["low"] for b in self._proxy_bars])
        closes = np.array([b["close"] for b in self._proxy_bars])

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
