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
        self._static_coins_set = set(static_coins)
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

    def _passes_basic_filter(self, ticker: dict) -> bool:
        """Базовые фильтры (без учёта объёма): USDT-пара и не в exclusion-листе."""
        symbol = ticker["symbol"]
        if "/USDT" not in symbol:
            return False
        if self._static_coins:
            return symbol in self._static_coins_set
        base = symbol.split("/")[0].upper()
        return base not in self._exclude_coins

    async def _collect_cycle(self) -> None:
        logger.info("Цикл сбора данных...")
        async with async_session() as session:
            # 1. Получаем тикеры со всех бирж
            all_tickers: dict[str, list[dict]] = {}
            for connector in self._connectors:
                try:
                    all_tickers[connector.exchange_id] = await connector.fetch_tickers()
                    logger.info(
                        f"{connector.exchange_id}: получено {len(all_tickers[connector.exchange_id])} тикеров"
                    )
                except Exception as e:
                    logger.warning(f"{connector.exchange_id}: не удалось получить тикеры: {e}")

            # 2. Строим symbol → {exchange: ticker} для кросс-биржевой фильтрации
            by_symbol: dict[str, dict[str, dict]] = {}
            for exchange_id, tickers in all_tickers.items():
                for t in tickers:
                    by_symbol.setdefault(t["symbol"], {})[exchange_id] = t

            # 3. Отбираем монеты, доступные на ByBit (торговая биржа)
            #    Объём проверяется по OR: достаточно на любой из бирж
            bybit_raw = all_tickers.get("bybit", [])
            selected_binance: list[dict] = []
            selected_bybit: list[dict] = []

            for t in bybit_raw:
                symbol = t["symbol"]
                tickers = by_symbol.get(symbol, {})
                bybit_t = tickers.get("bybit")
                if bybit_t is None:
                    continue

                if not self._passes_basic_filter(bybit_t):
                    continue

                # Объём: берём максимум из двух бирж (если монета есть на обеих)
                bybit_vol = bybit_t.get("volume") or 0
                binance_t = tickers.get("binance")
                binance_vol = (binance_t.get("volume") or 0) if binance_t else 0

                if max(bybit_vol, binance_vol) < self._min_volume:
                    continue

                if binance_t is not None:
                    selected_binance.append(binance_t)
                else:
                    selected_bybit.append(bybit_t)

            logger.info(
                f"bybit: {len(selected_binance)} общих с binance, "
                f"{len(selected_bybit)} уникальных (будут собраны)"
            )

            # 3.5. Сохраняем ВСЕ ByBit тикеры (нужны детектору для определения доступности)
            for t in bybit_raw:
                if self._passes_basic_filter(t):
                    session.add(Ticker(**t))

            # 4. Собираем данные
            for connector in self._connectors:
                try:
                    if connector.exchange_id == "binance":
                        await self._collect_for_exchange(connector, session, selected_binance)
                    elif connector.exchange_id == "bybit":
                        await self._collect_for_exchange(connector, session, selected_bybit)
                    else:
                        filtered = self._filter_tickers(all_tickers.get(connector.exchange_id, []))
                        await self._collect_for_exchange(connector, session, filtered)
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
