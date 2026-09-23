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


# ---------------------------------------------------------------------------
# Перенос полей замера из датакласса детектора в строку БД
# ---------------------------------------------------------------------------
#
# 22.09.2026 `volume_window_shifted` добавили в ORM-модель и в детектор, но
# перенос в `core/app.py` собирался явным списком полей, и поле в него не
# попало. Датакласс `Signal` не использует __slots__, поэтому присваивание
# неописанного атрибута не упало — колонка просто стояла NULL у всех сигналов,
# и замер, ради которого её вводили, сутки не собирался. Эти тесты закрывают
# именно этот класс ошибки: не «маппинг работает», а «новое поле нельзя
# добавить так, чтобы оно молча потерялось».


def _detector_signal(**over):
    from src.analytics.base import Signal

    fields = dict(symbol="X/USDT:USDT", setup_type="volume_surge", direction="long",
                  confidence=75, message="m")
    fields.update(over)
    return Signal(**fields)


def test_every_measurement_field_reaches_the_row():
    from datetime import datetime, timezone

    from src.storage.models import Signal as SignalModel

    sig = _detector_signal(closed_bar_ok=True, closed_bar_stage="volume_threshold",
                           last_bar_age_sec=104, volume_window_shifted=1)
    row = SignalModel.from_detector_signal(sig, datetime.now(tz=timezone.utc))

    assert [getattr(row, f) for f in SignalModel.MEASUREMENT_FIELDS] == [
        1, "volume_threshold", 104, 1
    ], "поле замера не доехало от датакласса до строки БД"


def test_measurement_fields_all_exist_on_the_dataclass():
    """Поле, объявленное в MEASUREMENT_FIELDS, обязано быть в датакласса —
    иначе перенос падал бы в бою на AttributeError, а не в тестах."""
    from dataclasses import fields

    from src.analytics.base import Signal
    from src.storage.models import Signal as SignalModel

    declared = {f.name for f in fields(Signal)}
    missing = [f for f in SignalModel.MEASUREMENT_FIELDS if f not in declared]
    assert not missing, f"нет в датаклассе analytics.base.Signal: {missing}"


def test_measurement_fields_all_exist_as_columns():
    """И обратная сторона: поле замера обязано быть колонкой, иначе оно
    потеряется на flush(), а не на присваивании."""
    from src.storage.models import Signal as SignalModel

    columns = set(SignalModel.__table__.columns.keys())
    missing = [f for f in SignalModel.MEASUREMENT_FIELDS if f not in columns]
    assert not missing, f"нет колонки в signals: {missing}"


def test_none_verdict_is_not_written_as_zero():
    """NULL («вердикт не определён») и 0 («сетапа на закрытых барах нет») —
    разные факты, и склеивать их нельзя: на этом различии стоит весь замер."""
    from datetime import datetime, timezone

    from src.storage.models import Signal as SignalModel

    row = SignalModel.from_detector_signal(_detector_signal(),
                                           datetime.now(tz=timezone.utc))
    assert row.closed_bar_ok is None
    assert row.volume_window_shifted is None


# ---------------------------------------------------------------------------
# Настройки SQLite
# ---------------------------------------------------------------------------
#
# До 23.09.2026 в проекте был выставлен ровно один PRAGMA — journal_mode=WAL, а
# всё остальное осталось на дефолтах SQLite. Дефолтный кеш (7.8 МБ) оказался
# РАВЕН рабочему множеству одного цикла сбора (6.6 МБ: 560 монет × три индекса
# с префиксом symbol), а порог чекпойнта WAL (3.9 МБ) — вдвое меньше него, то
# есть чекпойнт случался чаще, чем раз в цикл. Настройки легко потерять при
# правке `_set_journal_mode`, поэтому они закреплены тестом.


@pytest.mark.asyncio
async def test_sqlite_pragmas_are_configured():
    from src.storage.database import (
        CACHE_SIZE_KIB,
        WAL_AUTOCHECKPOINT_PAGES,
        engine,
    )

    async with engine.connect() as conn:
        async def pragma(name):
            return (await conn.exec_driver_sql(f"PRAGMA {name}")).fetchone()[0]

        assert (await pragma("journal_mode")) == "wal"
        # Отрицательное значение — кибибайты, положительное — страницы.
        assert (await pragma("cache_size")) == -CACHE_SIZE_KIB
        assert (await pragma("wal_autocheckpoint")) == WAL_AUTOCHECKPOINT_PAGES
        assert (await pragma("synchronous")) == 1, "NORMAL, а не дефолтный FULL"


@pytest.mark.asyncio
async def test_connection_pool_is_bounded():
    """Кеш страниц выделяется НА СОЕДИНЕНИЕ, а на VPS 1 ГБ памяти.

    Дефолтный пул SQLAlchemy (5 + 10 overflow) дал бы до 15 кешей по 16 МБ.
    """
    from src.storage.database import CACHE_SIZE_KIB, engine

    pool = engine.pool
    max_conns = pool.size() + pool._max_overflow
    assert max_conns <= 3, "пул не ограничен — потолок памяти под кеши уедет"
    assert max_conns * CACHE_SIZE_KIB / 1024 <= 64, "суммарный кеш больше 64 МБ"
