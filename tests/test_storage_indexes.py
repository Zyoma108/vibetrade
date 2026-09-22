"""Индексы должны доезжать до УЖЕ СУЩЕСТВУЮЩИХ таблиц.

Регресс деплоя 01.09.2026. `Base.metadata.create_all()` проверяет наличие
таблицы и, если она есть, пропускает её целиком вместе со всеми индексами.
На пустой БД новый индекс появляется, на боевой — никогда. В тот деплой на
хосте старый `ix_open_interest_exchange` удалился, а новый составной
`ix_oi_exchange_symbol_timestamp` не создался: главный фикс цикла молча не
применился, и заметить это по логам было нельзя — оно не падает и ничего не
пишет.
"""

import sqlite3

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.storage.database import _create_missing_indexes, _drop_obsolete_indexes
from src.storage.models import Base

OLD_SCHEMA = """
CREATE TABLE open_interest (
  id INTEGER NOT NULL PRIMARY KEY,
  exchange VARCHAR(32) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  timestamp DATETIME NOT NULL,
  value FLOAT NOT NULL
);
CREATE INDEX ix_open_interest_symbol ON open_interest (symbol);
CREATE INDEX ix_open_interest_exchange ON open_interest (exchange);
CREATE INDEX ix_open_interest_timestamp ON open_interest (timestamp);
"""


async def test_missing_index_is_added_to_existing_table(tmp_path):
    db_path = tmp_path / "old.db"
    raw = sqlite3.connect(db_path)
    raw.executescript(OLD_SCHEMA)
    raw.commit()
    raw.close()

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _create_missing_indexes(conn)
    await engine.dispose()

    names = [r[1] for r in sqlite3.connect(db_path).execute(
        "PRAGMA index_list(open_interest)"
    )]
    assert "ix_oi_exchange_symbol_timestamp" in names, (
        "составной индекс не доехал до существующей таблицы — "
        f"есть только {names}"
    )


async def test_is_idempotent_on_second_run(tmp_path):
    """Повторный старт не должен падать на уже существующих индексах."""
    db_path = tmp_path / "twice.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    for _ in range(2):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await _create_missing_indexes(conn)
    await engine.dispose()

    names = [r[1] for r in sqlite3.connect(db_path).execute(
        "PRAGMA index_list(open_interest)"
    )]
    assert names.count("ix_oi_exchange_symbol_timestamp") == 1


OLD_CANDLES = """
CREATE TABLE candles (
  id INTEGER NOT NULL PRIMARY KEY,
  exchange VARCHAR(32) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  timestamp DATETIME NOT NULL,
  open FLOAT NOT NULL, high FLOAT NOT NULL, low FLOAT NOT NULL,
  close FLOAT NOT NULL, volume FLOAT NOT NULL,
  CONSTRAINT uq_candle UNIQUE (exchange, symbol, timestamp)
);
CREATE INDEX ix_candles_exchange ON candles (exchange);
CREATE INDEX ix_candles_symbol ON candles (symbol);
CREATE INDEX ix_candles_timestamp ON candles (timestamp);
"""


async def test_obsolete_indexes_are_dropped_from_existing_db(tmp_path):
    """Лишние индексы должны сниматься с УЖЕ НАКОПЛЕННОЙ базы.

    create_all() их не уберёт никогда — он пропускает существующую таблицу
    целиком. А каждый лишний индекс обслуживается на КАЖДОЙ вставке: замер
    22.09.2026 на боевом снапшоте (9.6 млн строк) — цикл сбора OI по 600
    монетам 89 мс с тремя индексами против 17 мс без ix_open_interest_symbol.
    """
    db_path = tmp_path / "fat.db"
    raw = sqlite3.connect(db_path)
    raw.executescript(OLD_SCHEMA + OLD_CANDLES)
    raw.commit()
    raw.close()

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _create_missing_indexes(conn)
        await _drop_obsolete_indexes(conn)
    await engine.dispose()

    db = sqlite3.connect(db_path)
    names = {r[1] for table in ("open_interest", "candles")
             for r in db.execute(f"PRAGMA index_list({table})")}

    for gone in ("ix_open_interest_symbol", "ix_open_interest_exchange",
                 "ix_candles_exchange"):
        assert gone not in names, f"{gone} не удалён: {sorted(names)}"

    # Оставшиеся нужны: составной обслуживает горячие чтения, timestamp —
    # удаление по ретенции.
    assert "ix_oi_exchange_symbol_timestamp" in names
    assert "ix_open_interest_timestamp" in names
    assert "ix_candles_timestamp" in names


async def test_dropping_is_idempotent(tmp_path):
    """Повторный старт на уже почищенной базе не должен падать."""
    db_path = tmp_path / "clean.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    for _ in range(2):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await _create_missing_indexes(conn)
            await _drop_obsolete_indexes(conn)
    await engine.dispose()
