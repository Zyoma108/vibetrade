from pathlib import Path

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DB_PATH = Path("data/trading_bot.db")

engine = create_async_engine(
    f"sqlite+aiosqlite:///{DB_PATH}",
    echo=False,
    connect_args={"timeout": 10},  # ждать 10с вместо падения с "database is locked"
)


@event.listens_for(engine.sync_engine, "connect")
def _set_wal(dbapi_connection, connection_record):
    """Включить WAL-режим — конкурентные чтение и запись."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db() -> None:
    """Создать таблицы и недостающие колонки."""
    from src.storage.models import Base

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Добавляем новые колонки, если их ещё нет (для старых БД)
        for col_name, col_type in [
            ("tp_sl_set", "INTEGER DEFAULT 0"),
            ("partial_closed", "INTEGER DEFAULT 0"),
            ("partial_pnl", "FLOAT DEFAULT 0.0"),
        ]:
            try:
                await conn.exec_driver_sql(
                    f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}"
                )
            except Exception:
                pass  # колонка уже существует


async def get_session() -> AsyncSession:
    return async_session()
