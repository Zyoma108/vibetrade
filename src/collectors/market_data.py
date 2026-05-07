import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Coroutine

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
        coins: list[str],
        interval_seconds: int = 60,
        on_cycle_done: Callable[[AsyncSession], Coroutine] | None = None,
    ):
        self._connectors = connectors
        self._coins = coins
        self._interval = interval_seconds
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
                for coin in self._coins:
                    try:
                        await self._collect_coin(connector, coin, session)
                    except Exception:
                        logger.exception(
                            f"Ошибка сбора {coin} на {connector.exchange_id}"
                        )

            await session.commit()

            if self._on_cycle_done:
                await self._on_cycle_done(session)

    async def _collect_coin(
        self, connector: ExchangeConnector, symbol: str, session: AsyncSession
    ) -> None:
        # Свечи
        try:
            candles = await connector.fetch_ohlcv(symbol, limit=20)
            for c in candles:
                session.add(Candle(**c))
        except Exception as e:
            logger.warning(
                f"{connector.exchange_id}: не удалось получить свечи для {symbol}: {e}"
            )

        # Тикер
        try:
            ticker = await connector.fetch_ticker(symbol)
            session.add(Ticker(**ticker))
        except Exception as e:
            logger.warning(
                f"{connector.exchange_id}: не удалось получить тикер для {symbol}: {e}"
            )

        # Открытый интерес
        try:
            oi = await connector.fetch_open_interest(symbol)
            if oi is not None:
                session.add(OpenInterest(**oi))
        except Exception as e:
            logger.warning(
                f"{connector.exchange_id}: не удалось получить OI для {symbol}: {e}"
            )
