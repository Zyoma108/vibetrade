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

from src.storage.database import _create_missing_indexes
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
