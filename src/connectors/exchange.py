import asyncio
import logging
from datetime import datetime, timezone

import ccxt
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

FETCH_TIMEOUT = 30_000  # ms — сокетный таймаут ccxt, общий на весь объект биржи
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Политика для вызовов ЦИКЛА СБОРА (`scan=True`): короткий дедлайн, ноль ретраев.
#
# Торговые вызовы и вызовы сбора имеют противоположную цену ошибки. Потерять
# ордер нельзя — там 30 с и три попытки с backoff'ом оправданы. А пропустить
# свечи одной монеты в одном цикле стоит ~0: они приедут следующим циклом через
# полторы минуты. Зато ЖДАТЬ их стоит дорого: попытка держит слот семафора, и
# одна зависшая монета съедает 3 x 30 = 90 слото-секунд — столько же, сколько
# 273 здоровых запроса (замер 01.09.2026: p50 latency 0.33 с). Сорок таких
# монет за цикл дают наблюдавшиеся 800-1600 с вместо 78 с.
#
# Дедлайн ставится на стороне asyncio, а не через ccxt `timeout`: он у объекта
# биржи один на все потоки, менять его на лету — гонка. `asyncio.wait_for`
# отпускает слот семафора сразу, даже если поток ccxt ещё висит на сокете
# (поэтому и пул потоков, и пул HTTP-соединений держим с запасом — см.
# Application._setup_thread_pool и _widen_connection_pool).
#
# 8 -> 15 с по наблюдению 01.09.2026: при p50 latency 0.33 с и p95 0.66 с восемь
# секунд выглядели как двенадцатикратный запас, но сетевая заминка тормозит ВЕСЬ
# батч разом, а не отдельные монеты, — за одну такую заминку пропустилось 11
# монет bybit из 23. Верхнюю границу цикла держит не этот дедлайн, а
# SCAN_PHASE_TIMEOUT_SEC, поэтому запас здесь дешевле потерь: даже 15 с при
# нуле ретраев в шесть раз лучше прежних 3 x 30 = 90 слото-секунд.
SCAN_CALL_TIMEOUT_SEC = 15.0
SCAN_PHASE_TIMEOUT_SEC = 120.0  # верхняя граница на фазу фетча одной биржи

# Сколько сетевых вызовов к одной бирже держим в полёте одновременно.
# Было 5 — это 116 последовательных раундов на ~580 монет, то есть цикл равен
# 116 x латентность. Замер на публичном API bybit: 5 -> 11.8 req/s,
# 20 -> 34 req/s. Поднимать выше пула потоков бессмысленно (см. там же).
DEFAULT_CONCURRENCY = 20

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
        concurrency: int = DEFAULT_CONCURRENCY,
    ):
        exchange_class = getattr(ccxt, exchange_id)
        market_type = _DEFAULT_TYPE.get(exchange_id, "spot")

        config: dict = {
            "timeout": FETCH_TIMEOUT,
            "options": {"defaultType": market_type},
        }
        if api_key and secret:
            config.update({"apiKey": api_key, "secret": secret})

        self._exchange = exchange_class(config)
        self._widen_connection_pool(concurrency)
        self.exchange_id = exchange_id
        self._semaphore = asyncio.Semaphore(concurrency)
        self.concurrency = concurrency
        # Монеты, которых на этой бирже нет (ccxt.BadSymbol). Живут в tickers,
        # но каждый цикл гарантированно отдают ошибку — например ACX/USDT:USDT
        # на bybit. Ошибка мгновенная (ccxt проверяет по загруженным markets,
        # без сети), так что цена — только шум в логе и место в очереди фетча.
        # Сбрасывается перезапуском процесса: если монету на бирже наконец
        # листанули, бот подхватит её после ближайшего рестарта.
        self.unsupported_symbols: set[str] = set()

        if api_key:
            logger.info(
                f"{exchange_id}: trading connector создан (mainnet)"
            )

    def _widen_connection_pool(self, concurrency: int) -> None:
        """Согласовать пул HTTP-соединений с числом параллельных вызовов.

        Синхронный ccxt ходит через одну `requests.Session`, а её адаптер по
        умолчанию держит пул из 10 соединений. При concurrency=20 половина
        запросов не находит свободного соединения: urllib3 создаёт новое,
        отдаёт ответ и ВЫБРАСЫВАЕТ его, потому что класть обратно некуда.
        Каждый такой запрос платит полный TCP+TLS handshake, а лог заливается
        «Connection pool is full, discarding connection» — по строке на вызов
        (поймано на деплое 01.09.2026, сотни строк за цикл).

        Запас сверх конкурентности обязателен по той же причине, что и у пула
        потоков: `asyncio.wait_for` отпускает слот семафора по дедлайну, но
        поток ccxt продолжает висеть на сокете до своего FETCH_TIMEOUT и всё
        это время ДЕРЖИТ соединение. Без запаса следующая партия запросов
        снова упирается в полный пул — что и наблюдалось 01.09.2026 уже после
        первого фикса: `pool size: 20` при concurrency=20 и десятке зависших
        потоков.
        """
        session = getattr(self._exchange, "session", None)
        if session is None:  # ccxt без requests-транспорта — ничего не делаем
            return
        size = max(concurrency * 2, 20)
        adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

    @property
    def has_credentials(self) -> bool:
        return bool(self._exchange.apiKey)

    def _mark_unsupported(self, symbol: str) -> None:
        """Запомнить, что монеты на этой бирже нет — чтобы не спрашивать снова."""
        if symbol not in self.unsupported_symbols:
            self.unsupported_symbols.add(symbol)
            logger.info(
                f"{self.exchange_id}: {symbol} на бирже отсутствует — "
                f"исключена из сбора до перезапуска"
            )

    # ------------------------------------------------------------------
    # Low-level
    # ------------------------------------------------------------------

    async def _call(self, method_name: str, *args, scan: bool = False, **kwargs):
        """Вызов синхронного метода ccxt в потоке.

        `scan=True` — вызов из цикла сбора данных: дедлайн SCAN_CALL_TIMEOUT_SEC
        и ни одного ретрая. `scan=False` (по умолчанию) — торговый вызов: ждём
        до сокетного таймаута ccxt и повторяем MAX_RETRIES раз с backoff'ом.
        Почему политики разные — см. комментарий к SCAN_CALL_TIMEOUT_SEC.
        """
        attempts = 1 if scan else MAX_RETRIES
        last_error = None
        for attempt in range(attempts):
            try:
                async with self._semaphore:
                    method = getattr(self._exchange, method_name)
                    call = asyncio.to_thread(method, *args, **kwargs)
                    if scan:
                        return await asyncio.wait_for(call, SCAN_CALL_TIMEOUT_SEC)
                    return await call
            except (asyncio.TimeoutError, TimeoutError) as e:
                # Только scan-путь: поток ccxt может ещё висеть на сокете, но
                # слот семафора уже отпущен — цикл не ждёт эту монету.
                last_error = e
                logger.warning(
                    f"{self.exchange_id}: {method_name} не уложился в "
                    f"{SCAN_CALL_TIMEOUT_SEC:.0f}с — монета пропущена в этом цикле"
                )
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                last_error = e
                logger.warning(
                    f"{self.exchange_id}: попытка {attempt + 1}/{attempts} "
                    f"для {method_name} не удалась: {e}"
                )
                if attempt < attempts - 1:
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
        try:
            raw = await self._call("fetch_ohlcv", symbol, timeframe, scan=True, **kwargs)
        except ccxt.BadSymbol:
            self._mark_unsupported(symbol)
            raise
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
            try:
                raw = await self._call("fetch_open_interest", symbol, scan=True)
            except ccxt.BadSymbol:
                self._mark_unsupported(symbol)
                raise
        except (ccxt.BadRequest, ccxt.NotSupported, ccxt.ExchangeError):
            logger.debug(f"{self.exchange_id}: OI не поддерживается для {symbol}")
            return None
        return {
            "exchange": self.exchange_id,
            "symbol": symbol,
            "timestamp": datetime.now(tz=timezone.utc),  # свой timestamp (ByBit округляет до часа)
            "value": raw["openInterestAmount"],
        }

    async def fetch_tickers(self) -> list[dict]:
        """Забрать тикеры всех пар одним запросом.

        На linear-рынках (ByBit) тикер уже содержит открытый интерес
        (`info.openInterest`) — это тот же показатель, что отдаёт отдельный
        `fetch_open_interest()` (см. ccxt `parse_open_interest`: для linear
        `openInterestAmount` берётся из того же поля `openInterest`).
        Прокидываем его наружу как `open_interest`, чтобы вызывающий код
        мог не делать лишний round-trip на биржу за тем, что уже приехало.
        Ключ не входит в модель `Ticker` — вызывающий код обязан вынуть его
        (`dict.pop`) перед `Ticker(**t)`.
        """
        # Намеренно БЕЗ scan=True: это один вызов на цикл, а не на монету, —
        # усиления «зависшая монета съедает слот» здесь нет, зато полезная
        # нагрузка большая (~1200 тикеров) и в короткий дедлайн может не влезть.
        # Потерять её = остаться вообще без данных за цикл.
        raw = await self._call("fetch_tickers")
        result = []
        now = datetime.now(tz=timezone.utc)
        for symbol, data in raw.items():
            if not isinstance(data, dict):
                continue
            ts = data.get("timestamp")
            volume = data.get("quoteVolume") or data.get("baseVolume") or 0
            oi_raw = (data.get("info") or {}).get("openInterest")
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
                "open_interest": float(oi_raw) if oi_raw not in (None, "") else None,
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

    async def create_market_reduce_order(
        self, symbol: str, side: str, amount: float
    ) -> dict:
        """Рыночный reduce-only ордер — частичная или полная фиксация уже открытой
        позиции. Отдельно от `create_market_order`, потому что reduceOnly меняет
        смысл ордера: биржа гарантирует, что он только уменьшает позицию и не может
        случайно открыть встречную."""
        return await self._call(
            "create_order", symbol, "market", side, amount, None, {"reduceOnly": True}
        )

    async def fetch_open_orders(self, symbol: str) -> list[dict]:
        """Открытые ордера по символу. Бросает наружу — вызывающий код обязан
        отличать «ордеров нет» от «спросить не удалось»: подмена второго первым
        приводила к дублю reduce-only поверх живого лимитника (см.
        `PositionManager._check_partial_close_fallback`)."""
        return await self._call("fetch_open_orders", symbol)

    async def create_limit_order(
        self, symbol: str, side: str, amount: float, price: float
    ) -> dict:
        """Лимитный ордер на открытие позиции (не reduce-only).
        side = 'buy' | 'sell'. Используется для входа на откате
        (pending_entry_pullback_pct) вместо немедленного market-входа."""
        raw = await self._call("create_order", symbol, "limit", side, amount, price)
        logger.info(
            f"{self.exchange_id}: лимитник входа {side} {amount} {symbol} @ {price:.6f}"
        )
        return raw

    async def set_tpsl(
        self,
        symbol: str,
        side: str,
        amount: float,
        tp_price: float,
        sl_price: float,
        tp_as_limit: bool = False,
    ) -> dict:
        """Выставить TP/SL на открытую позицию (вызывается ПОСЛЕ ордера).

        `tp_as_limit` — закрыть TP лимитным ордером по цене tp_price вместо
        market: цена исполнения та же (TP и так закрывается по заранее
        известной цене), но комиссия maker (0.02%) вместо taker (0.055%).
        SL всегда остаётся market — риск непроскочить стоп при гэпе важнее
        экономии на комиссии."""
        close_side = "sell" if side == "buy" else "buy"
        params = {
            "takeProfitPrice": tp_price,
            "stopLossPrice": sl_price,
        }
        if tp_as_limit:
            params["takeProfitLimitPrice"] = tp_price
        raw = await self._call(
            "create_order", symbol, "market", close_side, amount,
            None, params
        )
        logger.info(
            f"{self.exchange_id}: TP/SL {symbol} "
            f"TP={tp_price:.6f}{'(limit)' if tp_as_limit else ''} SL={sl_price:.6f}"
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

    async def fetch_balance(self) -> dict:
        """Баланс аккаунта. Возвращает {'USDT': {'free': ..., 'used': ..., 'total': ...}}."""
        raw = await self._call("fetch_balance")
        return raw.get("USDT", raw.get("free", raw))

    async def min_order_amount(self, symbol: str) -> float | None:
        """Минимальный размер ордера (шаг precision / лимит биржи), либо None,
        если определить не удалось. Используется, чтобы не отправлять
        заведомо too-small ордер и не ловить ccxt.InvalidOrder ("amount ...
        must be greater than minimum amount precision") — на малом депозите
        risk_budget/price иногда меньше минимального лота (см. COHR/IREN,
        db-audit-august-2026)."""
        try:
            await self._call("load_markets")  # не сетевой вызов повторно, если уже загружены
            market = self._exchange.market(symbol)
            precision = (market.get("precision") or {}).get("amount")
            min_limit = ((market.get("limits") or {}).get("amount") or {}).get("min")
            candidates = [v for v in (precision, min_limit) if v]
            return max(candidates) if candidates else None
        except Exception as e:
            logger.debug(f"{self.exchange_id}: не удалось определить min amount для {symbol}: {e}")
            return None

    async def amount_to_precision(self, symbol: str, amount: float) -> float:
        """Округлить объём вниз до шага лота биржи; 0.0, если целого лота не набирается.

        Шаг может быть крупным: у SUI/USDT:USDT на bybit он равен 10 контрактам.
        ccxt всё равно обрежет объём до шага при отправке ордера, поэтому считать
        и записывать нужно уже обрезанное значение — иначе в БД остаётся объём,
        которого на бирже нет (27.08.2026: открыли 15.34, на бирже оказалось 10,
        и следующий же цикл принял разницу за исполнение партиал-лимитника).

        Ошибку ccxt ("must be greater than minimum amount precision") здесь
        превращаем в 0.0: для вызывающего это «такой объём не отправить», а не сбой.
        """
        try:
            await self._call("load_markets")
            return float(self._exchange.amount_to_precision(symbol, amount))
        except ccxt.InvalidOrder:
            return 0.0
        except Exception as e:
            logger.warning(
                f"{self.exchange_id}: не удалось округлить объём {amount} "
                f"для {symbol} до шага лота: {e}"
            )
            return float(amount)

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
            None, {"reduceOnly": True}
        )
        logger.info(
            f"{self.exchange_id}: закрыта позиция {symbol} "
            f"{close_side} {pos['contracts']}"
        )
        return raw

    async def place_reduce_only_limit(
        self, symbol: str, side: str, amount: float, price: float,
    ) -> dict | None:
        """Выставить reduce-only лимитный ордер (частичная фиксация).
        side = 'buy' | 'sell' — направление ПОЗИЦИИ (не ордера).
        amount — сколько контрактов закрыть.
        """
        close_side = "sell" if side == "buy" else "buy"
        params = {"reduceOnly": True}
        try:
            raw = await self._call(
                "create_order", symbol, "limit", close_side, amount, price, params
            )
            logger.info(
                f"{self.exchange_id}: лимитник reduce-only {symbol} "
                f"{close_side} {amount} @ {price:.6f}"
            )
            return raw
        except Exception:
            logger.exception(
                f"{self.exchange_id}: не удалось выставить лимитник для {symbol}"
            )
            return None

    async def cancel_all_orders(self, symbol: str) -> None:
        """Отменить все открытые ордера по символу (используется при закрытии позиции)."""
        try:
            open_orders = await self._call("fetch_open_orders", symbol)
            for order in open_orders:
                try:
                    await self._call("cancel_order", order["id"], symbol)
                    logger.info(
                        f"{self.exchange_id}: отменён ордер {order['id']} "
                        f"для {symbol}"
                    )
                except Exception:
                    logger.warning(
                        f"{self.exchange_id}: не удалось отменить ордер "
                        f"{order.get('id')} для {symbol}"
                    )
        except Exception:
            logger.exception(
                f"{self.exchange_id}: ошибка отмены ордеров для {symbol}"
            )

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
