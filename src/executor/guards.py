"""Защитные состояния бота: Circuit Breaker, чёрный список монет, кулдаун после
каскада ошибок — и их персист.

Выделено из `PositionManager` 26.08.2026: класс разросся до 1364 строк и пяти
почти не связанных обязанностей. Эта часть отделяется чище всего — с остальным
менеджером она связана только через `_notify`, а с БД имеет ровно одну строку
`bot_state`, то есть граница объект↔строка получается один к одному.

Персист был отдельной болью (см. `save()`): состояние жило только в памяти
процесса, любой рестарт бесшумно обнулял и защиту от серии убытков, и бан-лист
(db-audit-august-2026, P0), а первая попытка чинить это через собственную сессию
с ретраями упиралась в дедлок с транзакцией цикла сборщика.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import TradingConfig
from src.storage.database import async_session
from src.storage.models import BotState

logger = logging.getLogger(__name__)

# Все строки пишутся под этим source — колонка осталась от удалённого ИИ-режима
# (второй пайплайн на отдельном аккаунте), см. `Trade.source`.
SOURCE = "algo"


class TradingGuards:
    """Одна строка `bot_state` = один экземпляр. Владеет всем, что должно пережить
    рестарт: серией убытков, таймером полной остановки, бан-листом и кулдаунами."""

    # Задержки перед повторной попыткой при "database is locked" — только для пути
    # БЕЗ переданного session (см. docstring save() ниже).
    _SAVE_RETRY_DELAYS_SEC = (10, 20, 40, 80)

    def __init__(self, config: TradingConfig):
        self._config = config
        self.consecutive_losses: int = 0
        self.circuit_breaker_until: datetime | None = None
        # На каком значении consecutive_losses уже была выдана полная остановка — чтобы
        # после истечения таймера не остановить торговлю повторно на ТОЙ ЖЕ серии
        # убытков (сброс серии — только по факту победы, см. register_close).
        self.circuit_breaker_stop_consumed_at: int = 0
        self.banned_symbols: set[str] = set()
        self.error_counts: dict[str, int] = {}
        self.error_cooldown_until: dict[str, datetime] = {}

    # ------------------------------------------------------------------
    # Результат сделки
    # ------------------------------------------------------------------

    def register_loss(self) -> None:
        self.consecutive_losses += 1

    def register_win(self) -> None:
        """Победа — единственное, что сбрасывает серию убытков и снимает остановку."""
        self.consecutive_losses = 0
        self.circuit_breaker_until = None
        self.circuit_breaker_stop_consumed_at = 0

    # ------------------------------------------------------------------
    # Чёрный список и кулдаун ошибок
    # ------------------------------------------------------------------

    def ban_symbol(self, symbol: str) -> None:
        self.banned_symbols.add(symbol)

    def is_banned(self, symbol: str) -> bool:
        return symbol in self.banned_symbols

    def error_cooldown_left(self, symbol: str) -> datetime | None:
        """До какого момента символ в кулдауне, либо None если можно торговать."""
        until = self.error_cooldown_until.get(symbol)
        if until is not None and datetime.now(tz=timezone.utc) < until:
            return until
        return None

    def error_count(self, symbol: str) -> int:
        return self.error_counts.get(symbol, 0)

    async def load(self, session: AsyncSession) -> None:
        """Восстановить Circuit Breaker / бан-лист / error-cooldown из БД после
        рестарта процесса. Без этого вызова состояние всегда стартует "с чистого
        листа" (0 убытков подряд, пустой бан-лист) — вызывай один раз при старте,
        сразу после sync_positions (см. src/core/app.py)."""
        state = await session.get(BotState, SOURCE)
        if state is None:
            return
        self.consecutive_losses = state.consecutive_losses
        self.circuit_breaker_until = state.circuit_breaker_until
        self.circuit_breaker_stop_consumed_at = state.circuit_breaker_stop_consumed_at
        self.banned_symbols = set(json.loads(state.banned_symbols_json or "[]"))
        self.error_counts = json.loads(state.error_counts_json or "{}")
        self.error_cooldown_until = {
            symbol: datetime.fromisoformat(ts)
            for symbol, ts in json.loads(state.error_cooldown_until_json or "{}").items()
        }
        logger.info(
            f"Состояние восстановлено: "
            f"{self.consecutive_losses} убытков подряд, "
            f"{len(self.banned_symbols)} монет в чёрном списке, "
            f"{len(self.error_cooldown_until)} символов в кулдауне"
        )

    def _apply_to_row(self, state: BotState) -> None:
        """Перенести текущее in-memory состояние CB/бан-листа/error-cooldown в ORM-строку."""
        state.consecutive_losses = self.consecutive_losses
        state.circuit_breaker_until = self.circuit_breaker_until
        state.circuit_breaker_stop_consumed_at = self.circuit_breaker_stop_consumed_at
        state.banned_symbols_json = json.dumps(sorted(self.banned_symbols))
        state.error_counts_json = json.dumps(self.error_counts)
        state.error_cooldown_until_json = json.dumps(
            {symbol: dt.isoformat() for symbol, dt in self.error_cooldown_until.items()}
        )
        state.updated_at = datetime.now(tz=timezone.utc)

    async def save(self, session: AsyncSession | None = None) -> None:
        """Сохранить Circuit Breaker / бан-лист / error-cooldown в БД.

        Фикс 12.08.2026 (v2): изначально всегда открывала свою НЕЗАВИСИМУЮ сессию —
        но при вызове из `_close_position` (единственный "горячий" путь, срабатывает
        почти на КАЖДОМ закрытии сделки при включённом Circuit Breaker) это означало
        гонку с сессией `Application._on_collect_cycle_done`, которая держит одну
        транзакцию на весь ~5-минутный скан рынка и коммитит её только в конце —
        причём сама `_close_position` вызывается ИЗНУТРИ этой же транзакции. Retry с
        бэкоффом (150с бюджета, см. v1 этого фикса) не спасал: ждать было нечего —
        транзакция физически не могла закоммититься раньше, чем этот же await
        вернёт управление наверх по стеку, так что ожидание было эквивалентно
        затягиванию собственного дедлока до истечения бюджета ретраев.

        Теперь принимает опциональный `session` — если он передан (все вызовы из
        `_close_position`, у которой он есть в стеке через `update_positions`/
        ), пишем ПРЯМО В НЕГО (`flush`, без своего `commit` —
        закоммитится вместе с остальным в конце вызывающей транзакции). Это та же
        самая транзакция, гонки за блокировкой физически нет.

        Независимая сессия с ретраем остаётся fallback для вызовов БЕЗ session
        под рукой (`_track_error`/`_reset_errors` — синхронный код без session в
        стеке, см. `_schedule_save_state`) — там гонка с циклом сборщика всё ещё
        возможна, но эти пути срабатывают на порядок реже (только error-cascade),
        а не на каждом закрытии сделки."""
        if session is not None:
            try:
                state = await session.get(BotState, SOURCE)
                if state is None:
                    state = BotState(source=SOURCE)
                    session.add(state)
                self._apply_to_row(state)
                await session.flush()
            except Exception:
                logger.exception("Не удалось сохранить состояние Circuit Breaker")
            return

        attempt = 0
        while True:
            try:
                async with async_session() as own_session:
                    state = await own_session.get(BotState, SOURCE)
                    if state is None:
                        state = BotState(source=SOURCE)
                        own_session.add(state)
                    self._apply_to_row(state)
                    await own_session.commit()
                return
            except OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < len(self._SAVE_RETRY_DELAYS_SEC):
                    delay = self._SAVE_RETRY_DELAYS_SEC[attempt]
                    attempt += 1
                    logger.warning(
                        f"БД занята при сохранении состояния Circuit "
                        f"Breaker, повтор через {delay}с (попытка {attempt}/"
                        f"{len(self._SAVE_RETRY_DELAYS_SEC)})"
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.exception("Не удалось сохранить состояние Circuit Breaker")
                return
            except Exception:
                logger.exception("Не удалось сохранить состояние Circuit Breaker")
                return

    def schedule_save(self) -> None:
        """Запланировать фоновое сохранение состояния из синхронного кода.
        Безопасно вызывать и без работающего event loop (например, из
        синхронных unit-тестов) — тогда просто пропускаем персист."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        asyncio.create_task(self.save())

    def check_circuit_breaker(self) -> str | None:
        """Проверить, не заблокирована ли торговля Circuit Breaker'ом.

        Returns:
            None — можно торговать
            'circuit_breaker_stop' — полная остановка
            'circuit_breaker_reduce' — размер позиции уменьшен (торгуем дальше)
        """
        if not self._config.circuit_breaker_enabled:
            return None

        now = datetime.now(tz=timezone.utc)

        # Полная остановка?
        if self.circuit_breaker_until is not None:
            if now < self.circuit_breaker_until:
                return "circuit_breaker_stop"
            # Таймер истёк — снимаем полную блокировку, но серию убытков НЕ сбрасываем
            # (сброс — только по факту выигрыша, см. update_positions). Дальше проваливаемся
            # в проверку ниже: та же серия убытков переводит торговлю в reduce-режим
            # (уменьшенный размер), а не снова в stop — иначе это была бы бесконечная
            # остановка без единой сделки, способной её прервать победой.
            self.circuit_breaker_until = None
            logger.info(
                "Circuit Breaker: таймер остановки истёк, торговля возобновлена "
                f"уменьшенным размером ({self.consecutive_losses} убытков подряд не сброшены)"
            )

        # Новый убыток сверх уже "отработанной" остановки → полная остановка
        if (
            self.consecutive_losses >= self._config.circuit_breaker_loss_streak_stop
            and self.consecutive_losses > self.circuit_breaker_stop_consumed_at
        ):
            self.circuit_breaker_until = now + timedelta(
                minutes=self._config.circuit_breaker_stop_minutes
            )
            self.circuit_breaker_stop_consumed_at = self.consecutive_losses
            logger.warning(
                f"Circuit Breaker: {self.consecutive_losses} убытков подряд → "
                f"ПОЛНАЯ ОСТАНОВКА на {self._config.circuit_breaker_stop_minutes} мин "
                f"(до {self.circuit_breaker_until.strftime('%H:%M:%S')})"
            )
            # Метод синхронный (вызывается из горячего пути open_position) — сохраняем
            # состояние фоновой задачей, не блокируя проверку сигнала на запись в БД.
            self.schedule_save()
            return "circuit_breaker_stop"

        if self.consecutive_losses >= self._config.circuit_breaker_loss_streak_reduce:
            return "circuit_breaker_reduce"

        return None

    def position_size_mult(self) -> float:
        """Множитель размера позиции от Circuit Breaker."""
        cb_status = self.check_circuit_breaker()
        if cb_status == "circuit_breaker_reduce":
            return self._config.circuit_breaker_reduce_mult_pct / 100.0
        return 1.0

    def track_error(self, symbol: str) -> None:
        """Зафиксировать ошибку открытия позиции по символу.
        После 3 ошибок подряд — кулдаун 4 часа (защита от каскада)."""
        count = self.error_counts.get(symbol, 0) + 1
        self.error_counts[symbol] = count
        if count >= 3:
            cooldown_hours = 4
            self.error_cooldown_until[symbol] = (
                datetime.now(tz=timezone.utc)
                + timedelta(hours=cooldown_hours)
            )
            logger.warning(
                f"Error cascade: {symbol} — {count} ошибок подряд, "
                f"кулдаун на {cooldown_hours}ч"
            )
        # Метод синхронный (вызывается из мест без session под рукой, включая
        # обработку исключений биржи) — сохраняем фоновой задачей, см. _save_state.
        self.schedule_save()

    def reset_errors(self, symbol: str) -> None:
        """Сбросить счётчик ошибок после успешной сделки."""
        if symbol not in self.error_counts and symbol not in self.error_cooldown_until:
            return
        self.error_counts.pop(symbol, None)
        self.error_cooldown_until.pop(symbol, None)
        self.schedule_save()
