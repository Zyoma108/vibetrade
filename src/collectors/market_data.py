import asyncio
import logging
from typing import Callable, Coroutine

from sqlalchemy import select
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
            for connector in self._connectors:
                try:
                    await self._collect_for_exchange(connector, session)
                except Exception:
                    logger.exception(f"Ошибка сбора на {connector.exchange_id}")

            await session.commit()

            if self._on_cycle_done:
                await self._on_cycle_done(session)

    async def _collect_for_exchange(
        self, connector: ExchangeConnector, session: AsyncSession
    ) -> None:
        # 1. Собираем все тикеры одним запросом
        try:
            all_tickers = await connector.fetch_tickers()
        except Exception as e:
            logger.warning(f"{connector.exchange_id}: не удалось получить тикеры: {e}")
            return

        logger.info(f"{connector.exchange_id}: получено {len(all_tickers)} тикеров")

        # 2. Фильтруем по объёму и списку исключений
        selected = self._filter_tickers(all_tickers)
        logger.info(
            f"{connector.exchange_id}: после фильтра — {len(selected)} монет"
        )

        # 3. Сохраняем отфильтрованные тикеры
        for t in selected:
            session.add(Ticker(**t))

        # 4. Собираем свечи для всех отобранных монет
        oi_candidates: list[str] = []
        for t in selected:
            symbol = t["symbol"]
            try:
                candles = await connector.fetch_ohlcv(symbol, timeframe=self._timeframe, limit=100)
                # Сохраняем только новые свечи (дедупликация по exchange+symbol+timestamp)
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

                # Быстрая проверка: есть ли рост объёма в последних свечах?
                if len(candles) >= 4:
                    vols = [c["volume"] for c in candles[-4:]]
                    prev_vols = [c["volume"] for c in candles[-10:-4]]
                    if prev_vols and sum(prev_vols) > 0:
                        recent_avg = sum(vols) / len(vols)
                        prev_avg = sum(prev_vols) / len(prev_vols)
                        if recent_avg > prev_avg * 1.5:
                            oi_candidates.append(symbol)
            except Exception as e:
                logger.warning(
                    f"{connector.exchange_id}: свечи для {symbol}: {e}"
                )

        # 5. OI собираем только для кандидатов (где объём реально растёт)
        logger.info(
            f"{connector.exchange_id}: OI кандидатов — {len(oi_candidates)} "
            f"(из {len(selected)})"
        )
        for symbol in oi_candidates:
            try:
                oi = await connector.fetch_open_interest(symbol)
                if oi is not None:
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
