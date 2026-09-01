import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Coroutine

from sqlalchemy import delete, desc, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.connectors.exchange import SCAN_PHASE_TIMEOUT_SEC, ExchangeConnector
from src.storage.database import async_session
from src.storage.models import Candle, OpenInterest, Ticker

logger = logging.getLogger(__name__)

# Сколько монет записывать между коммитами. Коммит по монете (как было до
# 01.09.2026) — это ~1050 fsync'ов за цикл: по одному на монету в
# `_upsert_candles` и по одному на каждый изменившийся OI. На macOS fsync
# не сбрасывает кеш диска и стоит 0.39 мс, поэтому цена была незаметна в
# замерах; с настоящим барьером (Linux, `fullfsync=1`) это 21.3 мс, то есть
# ~22 с за цикл на ровном месте. Замер записи 583 строк OI на боевой БД:
#   коммит на монету — 12.53 с | чанк 25 — 0.55 с | чанк 50 — 0.31 с
#
# Коммитить всё одной транзакцией всё же нельзя: SQLAlchemy держит write-лок
# SQLite с первого автофлаша и до коммита, а лок на весь скан блокирует
# параллельных писателей (менеджер позиций, Telegram). Чанк — компромисс:
# лок держится доли секунды, а fsync'ов на два порядка меньше. Прежнее
# обоснование «коммит по монете» ссылалось на сетевые паузы внутри скана,
# но их здесь больше нет: фетч вынесен в отдельную фазу до записи.
COMMIT_CHUNK = 50

# Ретенция. Чистим раз в сутки и порциями: DELETE на миллион строк — это
# несколько секунд под write-локом, а лок здесь блокирует менеджер позиций.
#
# VACUUM намеренно НЕ делается. Он переписывает весь файл под эксклюзивной
# блокировкой — на многогигабайтной базе это минуты, в течение которых бот
# не может ни закрыть позицию, ни записать свечу. А чтобы остановить рост,
# он и не нужен: освободившиеся страницы SQLite переиспользует под новые
# строки сама. Файл перестаёт расти, даже если не уменьшается. Если место
# на диске всё же поджимает — VACUUM запускается руками на остановленном боте.
CLEANUP_INTERVAL = timedelta(hours=24)
CLEANUP_BATCH = 20_000


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
        retention_days: int = 0,
    ):
        self._connectors = connectors
        self._exclude_coins = set(name.upper() for name in exclude_coins)
        self._min_volume = min_volume_usdt
        self._interval = interval_seconds
        self._timeframe = timeframe
        self._on_cycle_done = on_cycle_done
        self._running = False
        self._task: asyncio.Task | None = None
        self._retention_days = retention_days
        self._last_cleanup: datetime | None = None

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
            try:
                await self._cleanup_old_data()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Ошибка чистки старых данных")
            await asyncio.sleep(self._interval)

    async def _cleanup_old_data(self) -> None:
        """Удалить свечи и OI старше `retention_days`. Раз в сутки, порциями.

        Своя сессия, а не сессия цикла: чистка не должна попадать внутрь
        транзакции сбора и удлинять её лок.
        """
        if self._retention_days <= 0:
            return
        now = datetime.now(tz=timezone.utc)
        if self._last_cleanup is not None and now - self._last_cleanup < CLEANUP_INTERVAL:
            return
        self._last_cleanup = now

        cutoff = now - timedelta(days=self._retention_days)
        t0 = time.perf_counter()
        removed: dict[str, int] = {}
        async with async_session() as session:
            for model in (Candle, OpenInterest):
                total = 0
                while True:
                    victims = (
                        select(model.id)
                        .where(model.timestamp < cutoff)
                        .limit(CLEANUP_BATCH)
                        .scalar_subquery()
                    )
                    result = await session.execute(
                        delete(model).where(model.id.in_(victims))
                    )
                    await session.commit()
                    if not result.rowcount:
                        break
                    total += result.rowcount
                if total:
                    removed[model.__tablename__] = total

        if removed:
            details = ", ".join(f"{k}: {v}" for k, v in removed.items())
            logger.info(
                f"Чистка истории старше {self._retention_days} сут "
                f"({cutoff:%Y-%m-%d}): удалено {details} "
                f"за {time.perf_counter() - t0:.1f}с"
            )

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
            bybit_to_save: list[dict] = []
            for t in bybit_raw:
                if self._passes_basic_filter(t):
                    oi_val = t.pop("open_interest", None)
                    if oi_val is not None:
                        oi_by_symbol[t["symbol"]] = oi_val
                    bybit_to_save.append(t)
            await self._upsert_tickers(session, bybit_to_save)
            await session.commit()

            # 4. Собираем данные
            for connector in self._connectors:
                try:
                    if connector.exchange_id == "binance":
                        await self._collect_for_exchange(connector, session, selected_binance)
                    elif connector.exchange_id == "bybit":
                        await self._collect_for_exchange(
                            connector, session, selected_bybit, oi_by_symbol=oi_by_symbol,
                            tickers_already_saved=True,
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
        tickers_already_saved: bool = False,
    ) -> None:
        """Собрать тикеры/свечи/OI для одной биржи.

        Свечи и (при необходимости) OI сначала фетчятся с биржи параллельно
        (сеть — узкое место, а `ExchangeConnector` уже ограничивает
        конкурентность своим семафором), а затем пишутся в БД одной короткой
        последовательной фазой без сетевых пауз между коммитами. Коммитим
        по монете, а не одним commit на всю биржу — иначе SQLAlchemy держит
        write-лок SQLite открытым с первого автофлаша и до конца всего скана,
        блокируя параллельных писателей. См. AGENTS.md, "База данных".

        `oi_by_symbol` — если передан (ByBit), OI уже есть в тикере
        (см. `ExchangeConnector.fetch_tickers`) и отдельный запрос на биржу
        за ним не делается.
        """
        logger.info(
            f"{connector.exchange_id}: сбор для {len(selected)} монет"
        )

        # 3. Сохраняем отфильтрованные тикеры. Для ByBit это уже сделано в
        # `_collect_cycle` (шаг 3.5 пишет ВСЕ его тикеры, `selected` — их подмножество),
        # поэтому здесь запись пропускается: раньше эти монеты писались дважды за цикл
        # и давали ~2.6% дублирующих строк.
        if not tickers_already_saved:
            await self._upsert_tickers(session, selected)
            await session.commit()

        # 4. Свечи: сначала параллельный фетч с биржи, потом быстрая последовательная запись
        logger.info(f"{connector.exchange_id}: сбор свечей для {len(selected)} монет...")
        t_fetch = time.perf_counter()
        candle_batches = await self._fetch_candles_concurrently(connector, selected)
        t_write = time.perf_counter()
        for i, (symbol, candles) in enumerate(candle_batches):
            if candles:
                await self._upsert_candles(session, symbol, candles)
            if (i + 1) % COMMIT_CHUNK == 0:
                await session.commit()
        await session.commit()
        t_oi = time.perf_counter()

        # 5. OI: для ByBit значение уже под рукой (получено бесплатно вместе с тикерами
        # в начале цикла) — без сетевого запроса. Для остальных бирж — параллельный фетч,
        # как и для свечей.
        if oi_by_symbol is not None:
            values = {
                t["symbol"]: oi_by_symbol[t["symbol"]]
                for t in selected
                if oi_by_symbol.get(t["symbol"]) is not None
            }
        else:
            logger.info(f"{connector.exchange_id}: сбор OI для {len(selected)} монет...")
            oi_batches = await self._fetch_oi_concurrently(connector, selected)
            values = {sym: oi["value"] for sym, oi in oi_batches if oi is not None}
        await self._write_oi_batch(session, connector.exchange_id, values)

        t_end = time.perf_counter()
        logger.info(
            f"{connector.exchange_id}: фазы — фетч свечей {t_write - t_fetch:.1f}с, "
            f"запись свечей {t_oi - t_write:.1f}с, OI {t_end - t_oi:.1f}с"
        )

    async def _upsert_tickers(self, session: AsyncSession, tickers: list[dict]) -> None:
        """Записать текущие тикеры — одна строка на (exchange, symbol), см. `Ticker`.

        Один statement на весь батч: при ~700 монетах за цикл поштучные SELECT+UPDATE
        стоили бы столько же round-trip-ов к SQLite, сколько весь остальной цикл.
        """
        if not tickers:
            return
        rows = [{k: v for k, v in t.items() if k != "open_interest"} for t in tickers]
        stmt = sqlite_insert(Ticker).values(rows)
        await session.execute(
            stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol"],
                set_={
                    c: getattr(stmt.excluded, c)
                    for c in ("timestamp", "bid", "ask", "last", "volume", "change_pct")
                },
            )
        )

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

        return await self._gather_with_deadline(
            connector, [fetch_one(t) for t in self._supported(connector, selected)],
            "свечи",
        )

    async def _upsert_candles(
        self, session: AsyncSession, symbol: str, candles: list[dict],
    ) -> None:
        """Записывает свечи одной монеты. Закрытые бары (старше последнего сохранённого)
        неизменны на бирже, поэтому не перепроверяются по одной — достаточно узнать
        максимальный уже сохранённый timestamp одним запросом, а не делать SELECT на
        каждую из ~100 полученных свечей.

        Не коммитит: коммит делает вызывающий раз в `COMMIT_CHUNK` монет (см. её
        комментарий). Запрос max(timestamp) ниже видит и ещё не закоммиченные строки
        текущего чанка — SQLAlchemy автофлашит их перед SELECT."""
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

        return await self._gather_with_deadline(
            connector, [fetch_one(t) for t in self._supported(connector, selected)],
            "OI",
        )

    @staticmethod
    def _supported(connector: ExchangeConnector, selected: list[dict]) -> list[dict]:
        """Отбросить монеты, которых на этой бирже нет (см.
        `ExchangeConnector.unsupported_symbols`)."""
        bad = connector.unsupported_symbols
        if not bad:
            return selected
        return [t for t in selected if t["symbol"] not in bad]

    @staticmethod
    async def _gather_with_deadline(
        connector: ExchangeConnector, coros: list, what: str,
    ) -> list:
        """Собрать результаты, но не дольше SCAN_PHASE_TIMEOUT_SEC на фазу.

        Верхняя граница на фазу — единственное, что делает каданс скана
        гарантией, а не вероятностью: дедлайн на отдельный вызов снижает шанс
        зависнуть, но 580 вызовов по 8 с в худшем случае всё равно дают
        неприемлемо длинный цикл. Что не успело — не собираем в этом цикле;
        свечи и OI приедут следующим. Ради этого фаза и отделена от записи в
        БД: отмена здесь ничего не рвёт, транзакций тут нет.
        """
        if not coros:
            return []
        tasks = [asyncio.ensure_future(c) for c in coros]
        done, pending = await asyncio.wait(tasks, timeout=SCAN_PHASE_TIMEOUT_SEC)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            logger.warning(
                f"{connector.exchange_id}: фаза «{what}» не уложилась в "
                f"{SCAN_PHASE_TIMEOUT_SEC:.0f}с — {len(pending)} из {len(tasks)} "
                f"монет пропущены до следующего цикла"
            )
        # Порядок задач не важен: вызывающий кладёт результаты в dict по symbol
        return [t.result() for t in tasks if t in done and not t.cancelled()]

    async def _write_oi_batch(
        self, session: AsyncSession, exchange: str, values: dict[str, float],
    ) -> None:
        """Сохраняет OI всей биржи с дедупликацией: только изменившиеся значения.

        Поиск последнего значения остаётся поштучным, а вот вставка и коммит —
        одни на всю биржу (было: коммит на каждую изменившуюся монету, ~500 за
        цикл, см. COMMIT_CHUNK про цену fsync'а).

        Поштучный SELECT здесь не случайность, а осознанный выбор против
        «одного запроса на все монеты» через `GROUP BY symbol`: тот вынужден
        просканировать весь раздел биржи (у binance это 1.05 млн строк), и его
        стоимость снова росла бы линейно с историей — ровно та беда, которую
        чинит составной индекс. Поштучный поиск по индексу — O(log n). Замер на
        боевой БД: батч-вариант 29.7 мс, 520 поштучных с индексом — доли
        миллисекунды. Без индекса поштучный стоил 13.7 мс НА МОНЕТУ.
        """
        if not values:
            return

        now = datetime.now(tz=timezone.utc)
        changed: list[dict] = []
        for symbol, value in values.items():
            last = await session.scalar(
                select(OpenInterest.value)
                .where(
                    OpenInterest.exchange == exchange,
                    OpenInterest.symbol == symbol,
                )
                .order_by(desc(OpenInterest.timestamp))
                .limit(1)
            )
            if last is None or last != value:
                changed.append({
                    "exchange": exchange, "symbol": symbol,
                    "timestamp": now, "value": value,
                })

        if changed:
            await session.execute(sqlite_insert(OpenInterest), changed)
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
