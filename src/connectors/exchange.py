import asyncio
import logging
from datetime import datetime, timezone

import ccxt

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = 30_000  # ms
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


class ExchangeConnector:
    """Обёртка над ccxt для работы с CEX-биржами."""

    def __init__(self, exchange_id: str):
        exchange_class = getattr(ccxt, exchange_id)
        self._exchange = exchange_class({"timeout": FETCH_TIMEOUT})
        self.exchange_id = exchange_id
        self._semaphore = asyncio.Semaphore(5)  # ограничиваем конкурентные запросы

    async def _call(self, method_name: str, *args, **kwargs):
        """Вызов синхронного метода ccxt в потоке с ретраями."""
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                async with self._semaphore:
                    method = getattr(self._exchange, method_name)
                    return await asyncio.to_thread(method, *args, **kwargs)
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                last_error = e
                logger.warning(
                    f"{self.exchange_id}: попытка {attempt + 1}/{MAX_RETRIES} "
                    f"для {method_name} не удалась: {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            except ccxt.BadRequest:
                raise
            except Exception:
                logger.exception(f"{self.exchange_id}: неожиданная ошибка в {method_name}")
                raise

        raise last_error  # type: ignore[misc]

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "5m", limit: int = 100
    ) -> list[dict]:
        raw = await self._call("fetch_ohlcv", symbol, timeframe, limit=limit)
        return [
            {
                "exchange": self.exchange_id,
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5],
            }
            for candle in raw
        ]

    async def fetch_ticker(self, symbol: str) -> dict:
        raw = await self._call("fetch_ticker", symbol)
        ts = raw.get("timestamp")
        return {
            "exchange": self.exchange_id,
            "symbol": symbol,
            "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            if ts
            else datetime.now(tz=timezone.utc),
            "bid": raw.get("bid"),
            "ask": raw.get("ask"),
            "last": raw["last"],
            "volume": raw.get("baseVolume"),
            "change_pct": raw.get("percentage"),
        }

    async def fetch_open_interest(self, symbol: str) -> dict | None:
        try:
            raw = await self._call("fetch_open_interest", symbol)
        except (ccxt.BadRequest, ccxt.NotSupported):
            logger.debug(f"{self.exchange_id}: OI не поддерживается для {symbol}")
            return None
        return {
            "exchange": self.exchange_id,
            "symbol": symbol,
            "timestamp": datetime.fromtimestamp(raw["timestamp"] / 1000, tz=timezone.utc)
            if raw.get("timestamp")
            else datetime.now(tz=timezone.utc),
            "value": raw["openInterestAmount"],
        }

    async def close(self) -> None:
        """Закрыть соединение с биржей."""
        if hasattr(self._exchange, "close"):
            try:
                await asyncio.to_thread(self._exchange.close)
            except Exception:
                pass
