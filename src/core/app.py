import asyncio
import logging
import signal
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import BaseDetector
from src.analytics.detector import SetupDetector
from src.collectors.market_data import MarketDataCollector
from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.executor.position_manager import PositionManager
from src.notifier.telegram_bot import TelegramNotifier
from src.storage.database import init_db
from src.storage.models import Signal as SignalModel

logger = logging.getLogger(__name__)


class Application:
    """Оркестратор торгового бота."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False
        self._connectors: list[ExchangeConnector] = []
        self._collector: MarketDataCollector | None = None
        self._detector: BaseDetector | None = None
        self._notifier: TelegramNotifier | None = None
        self._positions: PositionManager | None = None

    async def start(self) -> None:
        logger.info("Запуск приложения...")
        await init_db()

        # Коннекторы
        for ex_id, ex_cfg in self.settings.exchanges.items():
            if ex_cfg.enabled:
                self._connectors.append(ExchangeConnector(ex_id))
        logger.info(f"Биржи: {[c.exchange_id for c in self._connectors]}")

        # Аналитика
        self._detector = SetupDetector(self.settings.strategy)

        # Уведомления
        if self.settings.telegram.bot_token and self.settings.telegram.chat_ids:
            self._notifier = TelegramNotifier(self.settings.telegram)
            await self._notifier.start()
        else:
            logger.warning("Telegram не настроен (токен или chat_ids пусты), уведомления отключены")

        # Торговля (виртуальная или реальная)
        mode = self.settings.trading.mode
        if mode in ("virtual", "real"):
            self._positions = PositionManager(
                config=self.settings.trading,
                send_message=self._notifier.send_message if self._notifier else None,
            )
            logger.info(f"Менеджер позиций запущен (режим: {mode})")

        # Сборщик данных
        self._collector = MarketDataCollector(
            connectors=self._connectors,
            static_coins=self.settings.coins,
            exclude_coins=self.settings.strategy.exclude_coins,
            min_volume_usdt=self.settings.strategy.min_volume_usdt,
            interval_seconds=self.settings.collectors.interval_seconds,
            on_cycle_done=self._on_collect_cycle_done,
        )

        self._running = True
        await self._collector.start()
        logger.info("Приложение запущено")

    async def stop(self) -> None:
        logger.info("Остановка приложения...")
        self._running = False

        if self._collector:
            await self._collector.stop()

        if self._notifier:
            await self._notifier.stop()

        logger.info("Приложение остановлено")

    async def _on_collect_cycle_done(self, session: AsyncSession) -> None:
        """Вызывается после каждого цикла сбора данных."""

        # 1. Аналитика — ищем сетапы
        if self._detector:
            signals = await self._detector.analyze(session)
            for sig in signals:
                db_signal = SignalModel(
                    timestamp=datetime.now(tz=timezone.utc),
                    symbol=sig.symbol,
                    setup_type=sig.setup_type,
                    direction=sig.direction,
                    confidence=sig.confidence,
                    message=sig.message,
                )
                session.add(db_signal)

                # Открываем позицию (если торговля активна)
                trade = None
                if self._positions:
                    trade = await self._positions.open_position(session, sig)

                # Сигнал в Telegram — всегда, с пометкой о позиции
                if self._notifier:
                    await self._notifier.send_signal(sig, opened=bool(trade))

        # 2. Проверка TP/SL открытых позиций
        if self._positions:
            closed = await self._positions.update_positions(session)
            if closed:
                logger.info(f"Закрыто позиций за цикл: {len(closed)}")

        await session.commit()

    async def wait(self) -> None:
        """Ожидание graceful shutdown."""
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def handle_stop():
            logger.info("Получен сигнал остановки")
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handle_stop)
            except NotImplementedError:
                signal.signal(sig, lambda s, f: handle_stop())

        await stop_event.wait()
