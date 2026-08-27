"""
Тесты init_db() — стартового кода, который до 26.08.2026 не покрывался ничем.

Именно поэтому в прод уехал гейт схемы tickers, падавший на любой БД, включая
только что созданную: он искал индекс по имени `uq_ticker`, а SQLite для
inline-констрейнта заводит безымянный sqlite_autoindex_tickers_1. Тесты
дёргали Base.metadata.create_all напрямую и init_db() не вызывали ни разу.
"""

import sqlite3

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.storage import database


@pytest.fixture
def db_at(tmp_path, monkeypatch):
    """Переключить init_db() на временный файл БД."""

    def _use(name: str = "test.db"):
        path = tmp_path / name
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        monkeypatch.setattr(database, "DB_PATH", path)
        monkeypatch.setattr(database, "engine", engine)
        return path

    return _use


async def test_fresh_db_initializes(db_at):
    """Главная регрессия: на пустом каталоге init_db() обязан просто отработать."""
    path = db_at()
    await database.init_db()

    con = sqlite3.connect(path)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    con.close()
    assert "tickers" in tables


async def test_init_db_is_idempotent(db_at):
    """Перезапуск бота на уже созданной БД — тоже без исключений."""
    db_at()
    await database.init_db()
    await database.init_db()


async def test_old_ticker_schema_is_rejected(db_at):
    """Журнальная схема tickers (без уникального ключа) — upsert бы упал в рантайме."""
    path = db_at()
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE tickers (id INTEGER PRIMARY KEY, exchange VARCHAR(32) NOT NULL,"
        " symbol VARCHAR(32) NOT NULL, timestamp DATETIME NOT NULL, last FLOAT NOT NULL)"
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="старой схемы"):
        await database.init_db()


async def test_legacy_ticker_indexes_are_dropped(db_at):
    """Одиночные индексы старой схемы уводили планировщик в худший план."""
    path = db_at()
    await database.init_db()

    con = sqlite3.connect(path)
    con.execute("CREATE INDEX ix_tickers_exchange ON tickers (exchange)")
    con.execute("CREATE INDEX ix_tickers_timestamp ON tickers (timestamp)")
    con.commit()
    con.close()

    await database.init_db()

    con = sqlite3.connect(path)
    names = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tickers'"
    )}
    con.close()
    assert not names & {"ix_tickers_exchange", "ix_tickers_symbol", "ix_tickers_timestamp"}


class TestHasUniqueIndex:
    """Проверка ключа по составу колонок, а не по имени."""

    @staticmethod
    async def _check(path, table, columns):
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
        async with engine.begin() as conn:
            return await database._has_unique_index(conn, table, columns)

    async def test_finds_unnamed_autoindex(self, tmp_path):
        """SQLite назовёт индекс sqlite_autoindex_t_1 — по имени его не найти."""
        path = tmp_path / "a.db"
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE t (a TEXT, b TEXT, CONSTRAINT uq UNIQUE (a, b))")
        con.close()
        assert await self._check(path, "t", ("a", "b")) is True

    async def test_partial_column_match_is_not_enough(self, tmp_path):
        """Ключ по (a) не годится для ON CONFLICT (a, b)."""
        path = tmp_path / "b.db"
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE t (a TEXT UNIQUE, b TEXT)")
        con.close()
        assert await self._check(path, "t", ("a", "b")) is False

    async def test_non_unique_index_does_not_count(self, tmp_path):
        path = tmp_path / "c.db"
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE t (a TEXT, b TEXT)")
        con.execute("CREATE INDEX ix ON t (a, b)")
        con.close()
        assert await self._check(path, "t", ("a", "b")) is False
