import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import ccxt

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = 30_000  # ms
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Режим биржи по умолчанию — фьючерсы (нужен OI для стратегии)
_DEFAULT_TYPE: dict[str, str] = {
    "binance": "future",
    "bybit": "linear",
}


class ExchangeConnector:
    """Обёртка над ccxt для работы с CEX-биржами (public + trading)."""

    def __init__(
        self,
        exchange_id: str,
        api_key: str = "",
        secret: str = "",
        testnet: bool = False,
    ):
        exchange_class = getattr(ccxt, exchange_id)
        market_type = _DEFAULT_TYPE.get(exchange_id, "spot")

        config: dict = {
            "timeout": FETCH_TIMEOUT,
            "options": {"defaultType": market_type},
        }
        if api_key and secret:
            config.update({"apiKey": api_key, "secret": secret})
        if testnet:
            config["test"] = True

        self._exchange = exchange_class(config)
        self.exchange_id = exchange_id
        self._semaphore = asyncio.Semaphore(5)

        if api_key:
            net = "testnet" if testnet else "mainnet"
            logger.info(
                f"{exchange_id}: trading connector создан ({net})"
            )

    @property
    def has_credentials(self) -> bool:
        return bool(self._exchange.apiKey)

    # ------------------------------------------------------------------
    # Low-level
    # ------------------------------------------------------------------

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
            except (ccxt.BadRequest, ccxt.AuthenticationError, ccxt.ExchangeError):
                raise
            except Exception:
                logger.exception(f"{self.exchange_id}: неожиданная ошибка в {method_name}")
                raise

        raise last_error  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Public data
    # ------------------------------------------------------------------

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str = "5m", limit: int = 100,
        since: int | None = None,
    ) -> list[dict]:
        kwargs = {"limit": limit}
        if since is not None:
            kwargs["since"] = since
        raw = await self._call("fetch_ohlcv", symbol, timeframe, **kwargs)
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
        except (ccxt.BadRequest, ccxt.NotSupported, ccxt.ExchangeError):
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

    async def fetch_tickers(self) -> list[dict]:
        """Забрать тикеры всех пар одним запросом."""
        raw = await self._call("fetch_tickers")
        result = []
        now = datetime.now(tz=timezone.utc)
        for symbol, data in raw.items():
            if not isinstance(data, dict):
                continue
            ts = data.get("timestamp")
            volume = data.get("quoteVolume") or data.get("baseVolume") or 0
            result.append({
                "exchange": self.exchange_id,
                "symbol": symbol,
                "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                if ts else now,
                "bid": data.get("bid"),
                "ask": data.get("ask"),
                "last": data.get("last", 0),
                "volume": volume,
                "change_pct": data.get("percentage"),
            })
        return result

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    async def create_market_order(
        self, symbol: str, side: str, amount: float
    ) -> dict:
        """Рыночный ордер. side = 'buy' | 'sell'.
        Возвращает словарь с ключом 'fill_price' — фактическая цена исполнения."""
        raw = await self._call("create_order", symbol, "market", side, amount)
        # Фактическая цена: average (средневзвешенная) или price
        fill_price = raw.get("average") or raw.get("price")
        logger.info(
            f"{self.exchange_id}: market {side} {amount} {symbol} → "
            f"цена={fill_price}"
        )
        return {**raw, "fill_price": fill_price}

    async def set_tpsl(
        self,
        symbol: str,
        side: str,
        amount: float,
        tp_price: float,
        sl_price: float,
    ) -> dict:
        """Выставить TP/SL на открытую позицию (вызывается ПОСЛЕ ордера)."""
        close_side = "sell" if side == "buy" else "buy"
        params = {
            "takeProfitPrice": tp_price,
            "stopLossPrice": sl_price,
        }
        raw = await self._call(
            "create_order", symbol, "market", close_side, amount,
            None, params
        )
        logger.info(
            f"{self.exchange_id}: TP/SL {symbol} "
            f"TP={tp_price:.6f} SL={sl_price:.6f}"
        )
        return raw

    async def fetch_positions(self, symbol: str | None = None) -> list[dict]:
        """Открытые позиции на бирже."""
        args = ([symbol],) if symbol else ()
        raw = await self._call("fetch_positions", *args)
        result = []
        for p in raw:
            if isinstance(p, dict) and p.get("contracts", 0):
                result.append({
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "contracts": p["contracts"],
                    "entry_price": p.get("entryPrice", 0),
                    "unrealized_pnl": p.get("unrealizedPnl"),
                    "timestamp": datetime.fromtimestamp(
                        p["timestamp"] / 1000, tz=timezone.utc
                    ) if p.get("timestamp") else datetime.now(tz=timezone.utc),
                })
        return result

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Установить плечо для символа."""
        logger.info(f"{self.exchange_id}: устанавливаю плечо {leverage}x для {symbol}")
        await self._call("set_leverage", leverage, symbol)

    async def fetch_last_trade(
        self, symbol: str, since: datetime
    ) -> dict | None:
        """Последняя сделка по символу после указанного времени
        (нужна для определения фактической цены выхода)."""
        since_ts = int(since.timestamp() * 1000)
        trades = await self._call(
            "fetch_my_trades", symbol, since_ts, None, {"limit": 1}
        )
        if trades and len(trades) > 0:
            t = trades[-1]
            return {
                "price": t["price"],
                "amount": t["amount"],
                "side": t["side"],
                "timestamp": datetime.fromtimestamp(
                    t["timestamp"] / 1000, tz=timezone.utc
                ),
            }
        return None

    async def close_position(self, symbol: str) -> dict | None:
        """Закрыть позицию по рынку."""
        positions = await self.fetch_positions(symbol)
        if not positions:
            logger.info(f"{self.exchange_id}: нет открытой позиции для {symbol}")
            return None

        pos = positions[0]
        close_side = "sell" if pos["side"] == "long" else "buy"
        raw = await self._call(
            "create_order", symbol, "market", close_side, pos["contracts"],
            None, None, {"reduceOnly": True}
        )
        logger.info(
            f"{self.exchange_id}: закрыта позиция {symbol} "
            f"{close_side} {pos['contracts']}"
        )
        return raw

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Закрыть соединение с биржей."""
        if hasattr(self._exchange, "close"):
            try:
                await asyncio.to_thread(self._exchange.close)
            except Exception:
                pass
