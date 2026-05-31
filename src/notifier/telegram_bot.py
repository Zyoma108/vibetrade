import asyncio
import logging
from datetime import datetime
from typing import Callable, Coroutine

from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command

from src.analytics.base import Signal
from src.config import TelegramConfig

logger = logging.getLogger(__name__)

TELEGRAM_RETRY_DELAY = 10  # секунд между попытками отправки


class TelegramNotifier:
    """Отправка сигналов и уведомлений в Telegram."""

    def __init__(self, config: TelegramConfig):
        self._config = config
        self._bot = Bot(token=config.bot_token)
        self._dp = Dispatcher()
        self._paused = False
        self._online = False
        self._start_time: datetime | None = None
        self._signals_sent = 0
        self._polling_task: asyncio.Task | None = None
        self._stats_provider: Callable[[str], Coroutine] | None = None
        self._positions_provider: Callable[[], Coroutine] | None = None
        self._setup_handlers()

    def set_stats_provider(self, provider: Callable[[str], Coroutine]) -> None:
        self._stats_provider = provider

    def set_positions_provider(self, provider: Callable[[], Coroutine]) -> None:
        self._positions_provider = provider

    def _is_authorized(self, chat: types.Chat) -> bool:
        """Проверить, что сообщение из разрешённого чата/канала."""
        chat_id_str = str(chat.id)
        chat_username = f"@{chat.username}" if chat.username else None
        for allowed in self._config.chat_ids:
            if allowed == chat_id_str or allowed == chat_username:
                return True
        return False

    def _setup_handlers(self) -> None:
        @self._dp.message(Command("start"))
        async def start_handler(message: types.Message):
            """Всегда отвечает — нужно чтобы узнать свой ID для конфига."""
            username = f" (@{message.chat.username})" if message.chat.username else ""
            await message.answer(
                f"Привет! Твой chat ID: <code>{message.chat.id}</code>{username}\n\n"
                f"Пропиши этот ID в <code>chat_ids</code> в config.yaml, "
                f"чтобы бот отправлял тебе сигналы.",
                parse_mode="HTML",
            )

        @self._dp.message(Command("status"))
        async def status_handler(message: types.Message):
            if not self._is_authorized(message.chat):
                return
            uptime = ""
            if self._start_time:
                delta = datetime.now() - self._start_time
                hours, rem = divmod(int(delta.total_seconds()), 3600)
                minutes, seconds = divmod(rem, 60)
                uptime = f"{hours}ч {minutes}м {seconds}с"
            state = "⏸ Приостановлен" if self._paused else "🟢 Активен"
            await message.answer(
                f"Статус: {state}\n"
                f"Uptime: {uptime}\n"
                f"Сигналов отправлено: {self._signals_sent}"
            )

        @self._dp.message(Command("pause"))
        async def pause_handler(message: types.Message):
            if not self._is_authorized(message.chat):
                return
            if self._paused:
                await message.answer("Бот уже приостановлен.")
            else:
                self._paused = True
                await message.answer("⏸ Бот приостановлен. Сигналы не отправляются.")

        @self._dp.message(Command("resume"))
        async def resume_handler(message: types.Message):
            if not self._is_authorized(message.chat):
                return
            if not self._paused:
                await message.answer("Бот уже активен.")
            else:
                self._paused = False
                await message.answer("🟢 Бот возобновлён. Сигналы отправляются.")

        @self._dp.message(Command("stats"))
        async def stats_handler(message: types.Message):
            if not self._is_authorized(message.chat):
                return
            if not self._stats_provider:
                await message.answer("Статистика недоступна")
                return
            # Разбираем аргумент
            arg = message.text.strip().split()
            period = arg[1] if len(arg) > 1 else "day"
            if period not in ("day", "week", "month", "all"):
                await message.answer("Формат: /stats [day|week|month|all]")
                return
            text = await self._stats_provider(period)
            await message.answer(text, parse_mode="HTML")

        @self._dp.message(Command("positions"))
        async def positions_handler(message: types.Message):
            if not self._is_authorized(message.chat):
                return
            if not self._positions_provider:
                await message.answer("Информация о позициях недоступна")
                return
            text = await self._positions_provider()
            await message.answer(text, parse_mode="HTML")

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def is_online(self) -> bool:
        return self._online

    async def start(self) -> None:
        """Запустить polling бота (с retry при конфликте)."""
        self._start_time = datetime.now()

        # Сначала сбрасываем webhook и pending updates —
        # это заставляет Telegram освободить старые polling-сеансы
        for attempt in range(3):
            try:
                await self._bot.delete_webhook(drop_pending_updates=True)
                break
            except Exception as e:
                logger.warning(f"Не удалось сбросить webhook (попытка {attempt + 1}/3): {e}")
                await asyncio.sleep(2)

        max_retries = 8
        base_delay = 8  # базовая задержка между попытками, секунд

        for attempt in range(1, max_retries + 1):
            # Для повторных попыток — закрываем старую сессию и создаём свежий Bot
            if attempt > 1:
                await self._bot.session.close()
                await asyncio.sleep(2)
                self._bot = Bot(token=self._config.bot_token)

            self._polling_task = asyncio.create_task(
                self._dp.start_polling(
                    self._bot,
                    drop_pending_updates=True,
                    handle_signals=False,
                )
            )

            # Ждём успешного коннекта или фейла
            connected = False
            deadline = asyncio.get_event_loop().time() + 20  # 20 секунд на попытку
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(2)
                if self._polling_task.done():
                    exc = self._polling_task.exception()
                    if exc:
                        logger.warning(
                            f"Telegram polling не запустился "
                            f"(попытка {attempt}/{max_retries}): {exc}"
                        )
                    break
                try:
                    await self._bot.get_me()
                    self._online = True
                    connected = True
                    logger.info("Telegram-бот запущен")
                    break
                except TelegramNetworkError:
                    logger.warning("Telegram API недоступен, повторная попытка...")
                except Exception as e:
                    logger.warning(
                        f"Ошибка подключения к Telegram "
                        f"(попытка {attempt}/{max_retries}): {e}"
                    )
                    break

            if connected:
                break

            # Отменяем неудачную попытку перед retry
            if self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except (asyncio.CancelledError, Exception):
                    pass

            if attempt < max_retries:
                delay = base_delay * attempt  # 8с, 16с, 24с, 32с, ...
                logger.info(
                    f"Повторная попытка через {delay}с "
                    f"(сброс сессии и пересоздание Bot)..."
                )
                await asyncio.sleep(delay)

        if self._online:
            await self.notify_all("🟢 Торговый бот запущен")
        else:
            logger.error(
                f"Telegram-бот не смог подключиться после {max_retries} попыток. "
                f"Проверь:\n"
                f"  1) Нет ли другого экземпляра бота с этим же токеном\n"
                f"  2) Не запущен ли второй docker-контейнер: docker ps\n"
                f"  3) Не запущена ли локальная копия: ps aux | grep python\n"
                f"  4) Попробуй: docker-compose down && docker-compose up -d"
            )

    async def stop(self) -> None:
        """Остановить бота."""
        if self._online:
            await self.notify_all("🔴 Торговый бот остановлен")
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
        await self._bot.session.close()
        logger.info("Telegram-бот остановлен")

    async def send_signal(self, signal: Signal, status: str = "disabled") -> None:
        """Отправить торговый сигнал.
        status: opened | limit | duplicate | cooldown | no_price | error | disabled."""
        if self._paused:
            logger.info(f"Сигнал {signal.symbol} пропущен: бот приостановлен")
            return
        if not self._online:
            logger.info(f"Сигнал {signal.symbol} не отправлен: Telegram офлайн")
            return

        status_labels = {
            "opened": "🟢 Позиция открыта",
            "limit": "⚠️ Нет свободных слотов (лимит)",
            "duplicate": "⚠️ Уже есть позиция по монете",
            "cooldown": "⏳ Кулдаун после закрытия",
            "no_price": "⚠️ Нет цены для входа",
            "error": "❌ Ошибка создания ордера",
            "disabled": "ℹ️ Торговля отключена",
        }
        status_line = status_labels.get(status, f"⚠️ {status}")

        emoji = "📈" if signal.direction == "long" else "📉"
        text = (
            f"{emoji} <b>Сигнал: {signal.symbol}</b>\n"
            f"Тип сетапа: {signal.setup_type}\n"
            f"Направление: {signal.direction.upper()}\n"
            f"Уверенность: {signal.confidence}%\n"
            f"Статус: {status_line}\n\n"
            f"{signal.message}"
        )
        if await self.notify_all(text):
            self._signals_sent += 1
            logger.info(f"Сигнал отправлен: {signal.symbol} {signal.direction}")

    async def send_message(self, text: str) -> None:
        """Отправить произвольное сообщение."""
        if self._online:
            await self.notify_all(text)

    async def notify_all(self, text: str) -> bool:
        """Отправить сообщение всем чатам. Возвращает True если хотя бы одна отправка удалась."""
        if not self._online:
            return False
        success = False
        for chat_id in self._config.chat_ids:
            for attempt in range(3):
                try:
                    await self._bot.send_message(chat_id, text, parse_mode="HTML")
                    success = True
                    break
                except TelegramNetworkError:
                    logger.warning(
                        f"Telegram таймаут (попытка {attempt + 1}/3) для чата {chat_id}"
                    )
                    if attempt < 2:
                        await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                    else:
                        self._online = False
                except Exception as e:
                    logger.warning(f"Ошибка отправки в чат {chat_id}: {e}")
                    break
        return success
