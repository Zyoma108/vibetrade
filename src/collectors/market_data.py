import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Coroutine

from sqlalchemy import desc, func, select
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
        exclude_coins: list[str],
        min_volume_usdt: float,
        interval_seconds: int = 60,
        timeframe: str = "5m",
        on_cycle_done: Callable[[AsyncSession], Coroutine] | None = None,
    ):
        self._connectors = connectors
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

            # 3.5. Сохраняем ВСЕ ByBit тикеры (нужны детектору для определения доступности).
            # Заодно собираем OI, который уже приехал в тикере (см. ExchangeConnector.fetch_tickers) —
            # чтобы не делать по нему отдельный запрос на биржу ниже.
            oi_by_symbol: dict[str, float] = {}
            for t in bybit_raw:
                if self._passes_basic_filter(t):
                    oi_val = t.pop("open_interest", None)
                    if oi_val is not None:
                        oi_by_symbol[t["symbol"]] = oi_val
                    session.add(Ticker(**t))
            await session.commit()

            # 4. Собираем данные
            for connector in self._connectors:
                try:
                    if connector.exchange_id == "binance":
                        await self._collect_for_exchange(connector, session, selected_binance)
                    elif connector.exchange_id == "bybit":
                        await self._collect_for_exchange(
                            connector, session, selected_bybit, oi_by_symbol=oi_by_symbol,
                        )
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
        selected: list[dict], oi_by_symbol: dict[str, float] | None = None,
    ) -> None:
        """Собрать тикеры/свечи/OI для одной биржи.

        Свечи и (при необходимости) OI сначала фетчятся с биржи параллельно
        (сеть — узкое место, а `ExchangeConnector` уже ограничивает
        конкурентность своим семафором), а затем пишутся в БД одной короткой
        последовательной фазой без сетевых пауз между коммитами. Коммитим
        по монете, а не одним commit на всю биржу — иначе SQLAlchemy держит
        write-лок SQLite открытым с первого автофлаша и до конца всего скана,
        блокируя параллельных писателей (агент ИИ-режима). См. AGENTS.md,
        "База данных".

        `oi_by_symbol` — если передан (ByBit), OI уже есть в тикере
        (см. `ExchangeConnector.fetch_tickers`) и отдельный запрос на биржу
        за ним не делается.
        """
        logger.info(
            f"{connector.exchange_id}: сбор для {len(selected)} монет"
        )

        # 3. Сохраняем отфильтрованные тикеры
        for t in selected:
            t.pop("open_interest", None)
            session.add(Ticker(**t))
        await session.commit()

        # 4. Свечи: сначала параллельный фетч с биржи, потом быстрая последовательная запись
        logger.info(f"{connector.exchange_id}: сбор свечей для {len(selected)} монет...")
        candle_batches = await self._fetch_candles_concurrently(connector, selected)
        for symbol, candles in candle_batches:
            if candles:
                await self._upsert_candles(session, symbol, candles)

        # 5. OI: для ByBit значение уже под рукой (получено бесплатно вместе с тикерами
        # в начале цикла) — без сетевого запроса. Для остальных бирж — параллельный фетч,
        # как и для свечей.
        if oi_by_symbol is not None:
            for t in selected:
                await self._write_oi(
                    session, connector.exchange_id, t["symbol"], oi_by_symbol.get(t["symbol"]),
                )
        else:
            logger.info(f"{connector.exchange_id}: сбор OI для {len(selected)} монет...")
            oi_batches = await self._fetch_oi_concurrently(connector, selected)
            for symbol, oi in oi_batches:
                if oi is not None:
                    await self._write_oi(session, oi["exchange"], oi["symbol"], oi["value"])

    async def _fetch_candles_concurrently(
        self, connector: ExchangeConnector, selected: list[dict],
    ) -> list[tuple[str, list[dict] | None]]:
        """Параллельно (под семафором коннектора) фетчит свечи для всех монет —
        никаких обращений к БД здесь, только сеть."""
        async def fetch_one(t: dict) -> tuple[str, list[dict] | None]:
            symbol = t["symbol"]
            try:
                candles = await connector.fetch_ohlcv(symbol, timeframe=self._timeframe, limit=100)
                return symbol, candles
            except Exception as e:
                logger.warning(f"{connector.exchange_id}: свечи для {symbol}: {e}")
                return symbol, None

        return await asyncio.gather(*(fetch_one(t) for t in selected))

    async def _upsert_candles(
        self, session: AsyncSession, symbol: str, candles: list[dict],
    ) -> None:
        """Записывает свечи одной монеты. Закрытые бары (старше последнего сохранённого)
        неизменны на бирже, поэтому не перепроверяются по одной — достаточно узнать
        максимальный уже сохранённый timestamp одним запросом, а не делать SELECT на
        каждую из ~100 полученных свечей."""
        exchange = candles[0]["exchange"]
        max_ts = await session.scalar(
            select(func.max(Candle.timestamp)).where(
                Candle.exchange == exchange, Candle.symbol == symbol,
            )
        )
        for c in candles:
            # SQLite роняет tzinfo при round-trip (max_ts всегда naive), а свечи с биржи
            # приходят aware (UTC) — сравниваем по naive-представлению того же момента,
            # сам c["timestamp"] ниже (insert/update) не трогаем.
            ts = c["timestamp"]
            ts_naive = ts.replace(tzinfo=None) if ts.tzinfo is not None else ts
            if max_ts is not None and ts_naive < max_ts:
                continue  # закрытый бар уже сохранён и не меняется — пропускаем без запроса
            if max_ts is not None and ts_naive == max_ts:
                existing = await session.scalar(
                    select(Candle).where(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timestamp == c["timestamp"],
                    ).limit(1)
                )
                if existing is not None:
                    # Свеча уже была сохранена, но могла быть ещё не закрыта
                    # в момент первого опроса (interval_seconds << timeframe) —
                    # объём/high/low/close на бирже с тех пор могли вырасти.
                    # Обновляем, иначе в БД навсегда остаётся частичный объём
                    # "младенческой" свечи, что ломает фильтры по объёму.
                    existing.open = c["open"]
                    existing.high = c["high"]
                    existing.low = c["low"]
                    existing.close = c["close"]
                    existing.volume = c["volume"]
                    continue
            session.add(Candle(**c))
        await session.commit()

    async def _fetch_oi_concurrently(
        self, connector: ExchangeConnector, selected: list[dict],
    ) -> list[tuple[str, dict | None]]:
        """Параллельно (под семафором коннектора) фетчит OI для всех монет —
        только для бирж, где он не приехал бесплатно вместе с тикером."""
        async def fetch_one(t: dict) -> tuple[str, dict | None]:
            symbol = t["symbol"]
            try:
                oi = await connector.fetch_open_interest(symbol)
                return symbol, oi
            except Exception as e:
                logger.warning(f"{connector.exchange_id}: OI для {symbol}: {e}")
                return symbol, None

        return await asyncio.gather(*(fetch_one(t) for t in selected))

    async def _write_oi(
        self, session: AsyncSession, exchange: str, symbol: str, value: float | None,
    ) -> None:
        """Сохраняет OI с дедупликацией: только если значение изменилось."""
        if value is None:
            return
        last_oi = await session.scalar(
            select(OpenInterest.value)
            .where(OpenInterest.exchange == exchange, OpenInterest.symbol == symbol)
            .order_by(desc(OpenInterest.timestamp))
            .limit(1)
        )
        if last_oi is None or last_oi != value:
            session.add(OpenInterest(
                exchange=exchange, symbol=symbol,
                timestamp=datetime.now(tz=timezone.utc), value=value,
            ))
            await session.commit()

    def _filter_tickers(self, tickers: list[dict]) -> list[dict]:
        """Динамический отбор: USDT-пары, объём >= min, не в exclusion list."""
        result = []

        for t in tickers:
            symbol = t["symbol"]
            volume = t.get("volume") or 0

            if "/USDT" not in symbol:
                continue

            if volume < self._min_volume:
                continue

            base = symbol.split("/")[0].upper()
            if base in self._exclude_coins:
                continue

            result.append(t)

        return result
