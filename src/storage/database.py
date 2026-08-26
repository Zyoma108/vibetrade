import logging
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger(__name__)

DB_PATH = Path("data/trading_bot.db")

engine = create_async_engine(
    f"sqlite+aiosqlite:///{DB_PATH}",
    echo=False,
    connect_args={"timeout": 30},  # ждать 30с вместо падения с "database is locked" (DELETE-режим сериализует запись)
)


@event.listens_for(engine.sync_engine, "connect")
def _set_journal_mode(dbapi_connection, connection_record):
    """WAL-режим — конкурентные чтение и запись.

    История: 21-22.07.2026 БД дважды повреждалась под WAL поверх bind-mount
    тома Docker Desktop for Mac (`./data:/app/data`) — WAL полагается на
    shared-memory индекс (-shm) через mmap для координации между
    соединениями, а mmap/локи ненадёжны через osxfs/gRPC-FUSE. Временно
    переключали на DELETE (обычные файловые локи), но это сериализует запись
    целиком — основной цикл сборщика держит одну транзакцию на весь ~5-мин
    скан, и любой конкурентный писатель немедленно ловил "database is locked".

    Правильный фикс — `data/` теперь named Docker volume (`docker-compose.yml`,
    хранится в файловой системе Docker VM напрямую, не через host-bridge), на
    котором mmap работает штатно — поэтому WAL снова безопасен и восстановлен.
    См. AGENTS.md, "База данных".
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db() -> None:
    """Создать таблицы и недостающие колонки."""
    from src.storage.models import Base

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        # Проверить целостность базы
        import sqlite3
        try:
            db = sqlite3.connect(str(DB_PATH))
            result = db.execute("PRAGMA integrity_check").fetchone()
            db.close()
            if result[0] != "ok":
                raise RuntimeError(
                    f"База данных повреждена! integrity_check: {result[0]}\n"
                    f"Удали файл и перезапусти бота:\n"
                    f"  rm {DB_PATH.resolve()}"
                )
        except sqlite3.DatabaseError:
            raise RuntimeError(
                f"База данных повреждена и не читается!\n"
                f"Удали файл и перезапусти бота:\n"
                f"  rm {DB_PATH.resolve()}"
            )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Добавляем новые колонки, если их ещё нет (для старых БД)
        for col_name, col_type in [
            ("tp_sl_set", "INTEGER DEFAULT 0"),
            ("partial_closed", "INTEGER DEFAULT 0"),
            ("partial_pnl", "FLOAT DEFAULT 0.0"),
            ("missed_reason", "VARCHAR(32)"),
            ("missed_detail", "TEXT"),
            ("fee", "FLOAT DEFAULT 0.0"),
            ("pending_expires_at", "DATETIME"),
            ("source", "VARCHAR(16) DEFAULT 'algo'"),
            ("current_sl_price", "FLOAT"),
            ("signal_price", "FLOAT"),
        ]:
            try:
                await conn.exec_driver_sql(
                    f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}"
                )
            except Exception:
                pass  # колонка уже существует

        # Индексы tickers: create_all() создаёт недостающие, но никогда не убирает
        # лишние — старые БД тащат за собой одиночные индексы, из-за одного из
        # которых (ix_tickers_exchange) планировщик выбирал заведомо худший план
        # для `_get_current_price`. См. Ticker.__doc__.
        for idx in ("ix_tickers_exchange", "ix_tickers_symbol", "ix_tickers_timestamp"):
            try:
                await conn.exec_driver_sql(f"DROP INDEX IF EXISTS {idx}")
            except Exception:
                logger.warning(f"Не удалось удалить устаревший индекс {idx}")

        # tickers перестала быть журналом и стала снимком (одна строка на монету).
        # На БД, накопленной до этой смены, уникального ключа нет, и upsert упал бы
        # в рантайме с невнятным "ON CONFLICT clause does not match any ... UNIQUE
        # constraint". Проверяем на старте и говорим прямо, что делать.
        has_uq = await conn.exec_driver_sql(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name='tickers'"
            " AND name='uq_ticker'"
        )
        if not has_uq.first():
            raise RuntimeError(
                f"В {DB_PATH} таблица tickers из старой схемы (без уникального ключа "
                f"exchange+symbol). Начиная с 26.08.2026 это снимок, а не журнал — "
                f"история тикеров больше не хранится и не читается.\n"
                f"Удали файл БД и перезапусти бота:\n"
                f"  rm {DB_PATH.resolve()}"
            )
