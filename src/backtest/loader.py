"""
Загрузка исторических свечей для бэктеста.

Использование:
    python -m src.backtest.loader --days 7
    python -m src.backtest.loader --start 2026-05-11 --end 2026-05-18
"""

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Base, Candle

logger = logging.getLogger(__name__)

BACKTEST_DB = Path("data/backtest.db")


async def load_history(
    days: int = 7,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Загрузить исторические свечи в отдельную БД."""
    settings = Settings.from_yaml("config/config.yaml")

    # Отдельная БД для бэктеста
    BACKTEST_DB.parent.mkdir(parents=True, exist_ok=True)
    engine = create_async_engine(f"sqlite+aiosqlite:///{BACKTEST_DB}", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Период
    if start_date and end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(days=days)

    since_ms = int(start.timestamp() * 1000)
    logger.info(f"Период: {start} → {end}")

    # Коннектор к Binance
    conn = ExchangeConnector("binance")
    timeframe = settings.collectors.timeframe

    # Получаем тикеры и фильтруем
    logger.info("Получаем список тикеров с Binance...")
    all_tickers = await conn.fetch_tickers()
    exclude = set(c.upper() for c in settings.strategy.exclude_coins)

    filtered = []
    for t in all_tickers:
        symbol = t["symbol"]
        if "/USDT" not in symbol:
            continue
        base = symbol.split("/")[0].upper()
        if base in exclude:
            continue
        volume = t.get("volume") or 0
        if volume < settings.strategy.min_volume_usdt:
            continue
        filtered.append(symbol)

    logger.info(f"После фильтра: {len(filtered)} монет")

    total = len(filtered)
    stored = 0
    new_candles = 0

    # Загружаем свечи батчами (по 500 за запрос, с учётом since)
    BATCH_LIMIT = 500
    for i, symbol in enumerate(filtered):
        try:
            symbol_candles = 0
            batch_since_ms = since_ms  # начинаем с начала периода

            while batch_since_ms < int(end.timestamp() * 1000):
                batch = await conn.fetch_ohlcv(
                    symbol,
                    timeframe=timeframe,
                    since=batch_since_ms,
                    limit=BATCH_LIMIT,
                )

                if not batch:
                    break

                # Фильтруем по периоду
                batch = [c for c in batch if start <= c["timestamp"] <= end]
                if not batch and len(batch) < BATCH_LIMIT:
                    break  # достигли текущего момента

                # Сохраняем
                async with session_factory() as session:
                    for c in batch:
                        exists = await session.scalar(
                            select(Candle.id).where(
                                Candle.exchange == c["exchange"],
                                Candle.symbol == c["symbol"],
                                Candle.timestamp == c["timestamp"],
                            ).limit(1)
                        )
                        if not exists:
                            session.add(Candle(**c))
                            new_candles += 1
                            symbol_candles += 1
                    await session.commit()

                # Следующий батч: через 1 мс после последней свечи
                last_ts_ms = int(batch[-1]["timestamp"].timestamp() * 1000)
                batch_since_ms = last_ts_ms + 1

                if len(batch) < BATCH_LIMIT / 2:
                    break  # мало данных — конец истории

            stored += 1
            progress = f"[{i+1}/{total}]"
            print(f"\r{progress} {symbol}: {symbol_candles} свечей", end="")

            if (i + 1) % 50 == 0:
                await asyncio.sleep(1)

        except Exception as e:
            print(f"\r[{i+1}/{total}] {symbol}: ошибка — {e}")

    await conn.close()
    await engine.dispose()

    print(f"\n\nГотово. Загружено: {stored} монет, {new_candles} свечей")
    print(f"База: {BACKTEST_DB.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Загрузка истории для бэктеста")
    parser.add_argument("--days", type=int, default=7, help="Дней истории (по умолчанию 7)")
    parser.add_argument("--start", type=str, help="Начальная дата YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="Конечная дата YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    asyncio.run(load_history(
        days=args.days,
        start_date=args.start,
        end_date=args.end,
    ))


if __name__ == "__main__":
    main()
