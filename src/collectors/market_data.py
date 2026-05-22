import asyncio
import logging
from typing import Callable, Coroutine

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.connectors.exchange import ExchangeConnector
from src.storage.database import async_session
from src.storage.models import Candle, OpenInterest, Ticker

logger = logging.getLogger(__name__)


class MarketDataCollector:
    """Сбор рыночных данных с бирж по расписанию."""

    def __init__(
        self,
        connectors: list[ExchangeConnector],
        static_coins: list[str],
        exclude_coins: list[str],
        min_volume_usdt: float,
        interval_seconds: int = 60,
        timeframe: str = "5m",
        on_cycle_done: Callable[[AsyncSession], Coroutine] | None = None,
    ):
        self._connectors = connectors
        self._static_coins = static_coins
        self._exclude_coins = set(name.upper() for name in exclude_coins)
        self._min_volume = min_volume_usdt
        self._interval = interval_seconds
        self._timeframe = timeframe
        self._on_cycle_done = on_cycle_done
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("Сборщик данных запущен")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        for conn in self._connectors:
            await conn.close()
        logger.info("Сборщик данных остановлен")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._collect_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Ошибка в цикле сбора данных")
            await asyncio.sleep(self._interval)

    async def _collect_cycle(self) -> None:
        logger.info("Цикл сбора данных...")
        async with async_session() as session:
            # 1. Сначала получаем тикеры со всех бирж (параллельно)
            all_tickers: dict[str, list[dict]] = {}
            for connector in self._connectors:
                try:
                    all_tickers[connector.exchange_id] = await connector.fetch_tickers()
                    logger.info(
                        f"{connector.exchange_id}: получено {len(all_tickers[connector.exchange_id])} тикеров"
                    )
                except Exception as e:
                    logger.warning(f"{connector.exchange_id}: не удалось получить тикеры: {e}")

            # 2. Вычисляем уникальные монеты ByBit (которых нет на Binance)
            binance_symbols: set[str] = set()
            if "binance" in all_tickers:
                binance_tickers = self._filter_tickers(all_tickers["binance"])
                binance_symbols = {t["symbol"] for t in binance_tickers}

            bybit_shared: list[dict] = []
            bybit_unique: list[dict] = []
            if "bybit" in all_tickers:
                all_bybit = self._filter_tickers(all_tickers["bybit"])
                for t in all_bybit:
                    if t["symbol"] in binance_symbols:
                        bybit_shared.append(t)
                    else:
                        bybit_unique.append(t)
                logger.info(
                    f"bybit: {len(bybit_shared)} общих с binance, "
                    f"{len(bybit_unique)} уникальных (будут собраны)"
                )

            # 3. Собираем данные
            for connector in self._connectors:
                try:
                    if connector.exchange_id == "bybit":
                        # ByBit: только уникальные монеты
                        await self._collect_for_exchange(connector, session, bybit_unique)
                    elif connector.exchange_id == "binance":
                        # Binance: все монеты, которые есть на ByBit
                        bybit_all = {t["symbol"] for t in (bybit_shared + bybit_unique)}
                        filtered = [t for t in all_tickers.get("binance", [])
                                    if t["symbol"] in bybit_all]
                        selected = self._filter_tickers(filtered)
                        await self._collect_for_exchange(connector, session, selected)
                    else:
                        selected = self._filter_tickers(all_tickers.get(connector.exchange_id, []))
                        await self._collect_for_exchange(connector, session, selected)
                except Exception:
                    logger.exception(f"Ошибка сбора на {connector.exchange_id}")

            await session.commit()

            if self._on_cycle_done:
                await self._on_cycle_done(session)

    async def _collect_for_exchange(
        self, connector: ExchangeConnector, session: AsyncSession,
        selected: list[dict],
    ) -> None:
        logger.info(
            f"{connector.exchange_id}: сбор для {len(selected)} монет"
        )

        # 3. Сохраняем отфильтрованные тикеры
        for t in selected:
            session.add(Ticker(**t))

        # 4. Собираем свечи для всех отобранных монет
        for t in selected:
            symbol = t["symbol"]
            try:
                candles = await connector.fetch_ohlcv(symbol, timeframe=self._timeframe, limit=100)
                for c in candles:
                    exists = await session.scalar(
                        select(Candle.id).where(
                            Candle.exchange == c["exchange"],
                            Candle.symbol == c["symbol"],
                            Candle.timestamp == c["timestamp"],
                        ).limit(1)
                    )
                    if not exists:
                        session.add(Candle(**c))
            except Exception as e:
                logger.warning(
                    f"{connector.exchange_id}: свечи для {symbol}: {e}"
                )

        # 5. OI собираем для всех монет (с дедупликацией: только если значение изменилось)
        logger.info(
            f"{connector.exchange_id}: сбор OI для {len(selected)} монет..."
        )
        for t in selected:
            symbol = t["symbol"]
            try:
                oi = await connector.fetch_open_interest(symbol)
                if oi is not None:
                    # Не сохраняем дубликат, если значение не изменилось
                    last_oi = await session.scalar(
                        select(OpenInterest.value)
                        .where(
                            OpenInterest.exchange == oi["exchange"],
                            OpenInterest.symbol == oi["symbol"],
                        )
                        .order_by(desc(OpenInterest.timestamp))
                        .limit(1)
                    )
                    if last_oi is None or last_oi != oi["value"]:
                        session.add(OpenInterest(**oi))
            except Exception as e:
                logger.warning(
                    f"{connector.exchange_id}: OI для {symbol}: {e}"
                )

    def _filter_tickers(self, tickers: list[dict]) -> list[dict]:
        result = []

        # Если задан статический список — используем только его
        if self._static_coins:
            static_set = set(self._static_coins)
            for t in tickers:
                if t["symbol"] in static_set:
                    result.append(t)
            return result

        # Динамический отбор: USDT-пары, объём >= min, не в exclusion list
        for t in tickers:
            symbol = t["symbol"]
            volume = t.get("volume") or 0

            # Только пары к USDT (не USDC, не BTC и т.д.)
            if "/USDT" not in symbol:
                continue

            if volume < self._min_volume:
                continue

            base = symbol.split("/")[0].upper()
            if base in self._exclude_coins:
                continue

            result.append(t)

        return result
