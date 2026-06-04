import asyncio
import logging
import signal
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import BaseDetector
from src.analytics.data_provider import DataProvider
from src.analytics.detector import SetupDetector
from src.analytics.market_context import MarketContext
from src.analytics.price_surge import PriceSurgeDetector
from src.analytics.price_surge_service import PriceSurgeSignalProcessor
from src.collectors.market_data import MarketDataCollector
from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.executor.position_manager import PositionManager
from src.notifier.telegram_bot import TelegramNotifier
from src.storage.database import async_session, init_db
from src.storage.stats import trade_stats
from src.storage.models import Signal as SignalModel

logger = logging.getLogger(__name__)


class Application:
    """Оркестратор торгового бота."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False
        self._connectors: list[ExchangeConnector] = []
        self._trading_connector: ExchangeConnector | None = None
        self._collector: MarketDataCollector | None = None
        self._detector: BaseDetector | None = None
        self._detector_price_surge: PriceSurgeDetector | None = None
        self._market_ctx: MarketContext | None = None
        self._notifier: TelegramNotifier | None = None
        self._notifier_price_surge: TelegramNotifier | None = None
        self._ps_processor: PriceSurgeSignalProcessor | None = None
        self._positions: PositionManager | None = None

    async def start(self) -> None:
        logger.info("Запуск приложения...")
        await init_db()

        mode = self.settings.trading.mode
        trading_exchange = self.settings.trading.exchange

        # Коннекторы для сбора данных
        for ex_id, ex_cfg in self.settings.exchanges.items():
            if ex_cfg.enabled:
                self._connectors.append(ExchangeConnector(ex_id))
        logger.info(f"Биржи (данные): {[c.exchange_id for c in self._connectors]}")

        # Коннектор для торговли (real)
        if mode == "real":
            ex_cfg = self.settings.exchanges.get(trading_exchange)
            if not ex_cfg or not ex_cfg.api_key:
                logger.error(
                    f"Real-режим: не настроены API-ключи для {trading_exchange}. "
                    f"Проверь config.yaml"
                )
                raise RuntimeError("Real-режим требует API-ключи")

            self._trading_connector = ExchangeConnector(
                exchange_id=trading_exchange,
                api_key=ex_cfg.api_key,
                secret=ex_cfg.secret,
                testnet=ex_cfg.testnet,
            )
            net = "TESTNET" if ex_cfg.testnet else "MAINNET"
            logger.info(f"Торговый коннектор: {trading_exchange} ({net})")

        # Аналитика
        self._detector = SetupDetector(
            self.settings.strategy,
            timeframe=self.settings.collectors.timeframe,
        )

        # Вторая стратегия (только сигналы, без торговли)
        if self.settings.strategy_price_surge:
            self._detector_price_surge = PriceSurgeDetector(
                self.settings.strategy_price_surge,
                timeframe=self.settings.collectors.timeframe,
            )
            logger.info(
                "PriceSurge детектор: +{:.0f}% за {} мин".format(
                    self.settings.strategy_price_surge.price_surge_pct,
                    self.settings.strategy_price_surge.price_surge_minutes,
                )
            )

        # Рыночный контекст (BTC + OTHERS Supertrend) — использует свой коннектор к бирже
        if self._connectors:
            self._market_ctx = MarketContext(
                self.settings.market_context,
                connector=self._connectors[0],  # любой public-коннектор (без API-ключей)
            )
            logger.info("MarketContext инициализирован")

        # Уведомления
        if self.settings.telegram.bot_token and self.settings.telegram.chat_ids:
            self._notifier = TelegramNotifier(self.settings.telegram)
            await self._notifier.start()

            # Провайдер статистики
            async def stats_provider(period: str) -> str:
                async with async_session() as s:
                    return await trade_stats(s, period)

            self._notifier.set_stats_provider(stats_provider)

            # Провайдер позиций
            async def positions_provider() -> str:
                if not self._positions:
                    return "Торговля не активна"
                if not self._trading_connector:
                    return "Торговля не активна (нет подключения к бирже)"

                try:
                    ex_positions = await self._trading_connector.fetch_positions()
                except Exception as e:
                    return f"Ошибка получения позиций: {e}"

                if not ex_positions:
                    return "📋 Нет открытых позиций"

                lines = ["📋 <b>Открытые позиции</b>\n"]
                for p in ex_positions:
                    entry = p["entry_price"]
                    qty = abs(p["contracts"])
                    # Текущая цена из тикера
                    async with async_session() as s:
                        price = await self._positions._get_current_price(s, p["symbol"])
                    current = price or entry
                    upnl = (current - entry) * qty
                    roi = (current / entry - 1) * 100
                    emoji = "🟢" if upnl >= 0 else "🔴"
                    lines.append(
                        f"{emoji} <b>{p['symbol']}</b>\n"
                        f"  Вход: ${entry:.6f} → Тек: ${current:.6f}\n"
                        f"  PnL: ${upnl:+.2f} | ROI: {roi:+.1f}%"
                    )
                return "\n".join(lines)

            self._notifier.set_positions_provider(positions_provider)

            # Провайдер тренда
            async def trend_provider() -> str:
                if not self._market_ctx:
                    return "Рыночный контекст недоступен"
                return self._market_ctx.trend_summary()

            self._notifier.set_trend_provider(trend_provider)
        else:
            logger.warning("Telegram не настроен, уведомления отключены")

        # Второй Telegram-бот для strategy_price_surge
        if self.settings.telegram_price_surge and self.settings.telegram_price_surge.bot_token and self.settings.telegram_price_surge.chat_ids:
            self._notifier_price_surge = TelegramNotifier(self.settings.telegram_price_surge)
            await self._notifier_price_surge.start()
            logger.info("Telegram-бот Price Surge запущен")

            if self._detector_price_surge:
                self._ps_processor = PriceSurgeSignalProcessor(
                    config=self.settings.strategy_price_surge,
                    detector=self._detector_price_surge,
                    notifier=self._notifier_price_surge,
                    timeframe=self.settings.collectors.timeframe,
                )
                logger.info("PriceSurgeSignalProcessor инициализирован")
        elif self.settings.strategy_price_surge:
            logger.warning("Telegram Price Surge не настроен, сигналы strategy_price_surge не будут отправлены")

        # Торговля (virtual или real)
        if mode in ("virtual", "real"):
            self._positions = PositionManager(
                config=self.settings.trading,
                send_message=self._notifier.send_message if self._notifier else None,
                trading_connector=self._trading_connector,
            )
            logger.info(f"Менеджер позиций запущен (режим: {mode})")

            # Синхронизация с биржей при старте (real)
            if mode == "real":
                try:
                    async with async_session() as session:
                        await self._positions.sync_positions(session)
                        await session.commit()
                except Exception:
                    logger.exception(
                        "Не удалось синхронизировать позиции. "
                        "Бот продолжит работу в режиме сбора данных"
                    )

        # Первичное обновление рыночного контекста и отправка в Telegram
        if self._market_ctx and self.settings.market_context.enabled:
            try:
                async with async_session() as session:
                    await self._market_ctx.update(session, force=True)
                if self._market_ctx.ready and self._notifier:
                    await self._notifier.send_message(
                        "📊 <b>Рыночный контекст при старте</b>\n\n"
                        + self._market_ctx.trend_summary()
                    )
            except Exception:
                logger.exception("Не удалось обновить MarketContext при старте")

        # Сборщик данных
        self._collector = MarketDataCollector(
            connectors=self._connectors,
            static_coins=self.settings.coins,
            exclude_coins=self.settings.strategy.exclude_coins,
            min_volume_usdt=self.settings.strategy.min_volume_usdt,
            interval_seconds=self.settings.collectors.interval_seconds,
            timeframe=self.settings.collectors.timeframe,
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

        if self._trading_connector:
            await self._trading_connector.close()

        if self._notifier:
            await self._notifier.stop()

        if self._notifier_price_surge:
            await self._notifier_price_surge.stop()

        logger.info("Приложение остановлено")

    async def _on_collect_cycle_done(self, session: AsyncSession) -> None:
        """Вызывается после каждого цикла сбора данных."""

        # Создаём общий DataProvider на цикл — оба детектора кешируют данные в нём
        dp = DataProvider()
        if self._detector:
            self._detector.data_provider = dp
        if self._detector_price_surge:
            self._detector_price_surge.data_provider = dp
        if self._ps_processor:
            self._ps_processor.data_provider = dp

        # 0. Обновление рыночного контекста (BTC + OTHERS Supertrend)
        if self._market_ctx:
            await self._market_ctx.update(session)

            # Уведомление о смене рыночного режима
            if self._market_ctx.regime_changed and self._notifier:
                await self._notifier.send_message(
                    "🔄 <b>Смена рыночного режима!</b>\n\n"
                    + self._market_ctx.trend_summary()
                )

            # Уведомление о смене тренда (для TP/трейлинг стопов)
            if self._market_ctx.trend_changed and self._notifier:
                prev_trend_emoji = {
                    "bullish": "🟢 BULLISH",
                    "bearish": "🔴 BEARISH",
                    "neutral": "⚪ NEUTRAL",
                }.get(self._market_ctx.prev_trend, str(self._market_ctx.prev_trend))
                current_trend_emoji = {
                    "bullish": "🟢 BULLISH",
                    "bearish": "🔴 BEARISH",
                    "neutral": "⚪ NEUTRAL",
                }.get(self._market_ctx.trend, self._market_ctx.trend)
                await self._notifier.send_message(
                    f"📈 <b>Смена тренда!</b>\n\n"
                    f"{prev_trend_emoji} → {current_trend_emoji}\n\n"
                    + self._market_ctx.trend_summary()
                )

        # Передаём контекст в PositionManager
        if self._positions and self._market_ctx:
            self._positions.market_regime = self._market_ctx.regime
            self._positions.position_size_mult = (
                self._market_ctx.position_size_multiplier()
            )

        # 1. Синхронизация позиций с биржей (каждый цикл, перед аналитикой)
        if self._positions:
            closed = await self._positions.update_positions(session)
            if closed:
                logger.info(f"Закрыто позиций за цикл: {len(closed)}")

        # 2. Аналитика — основная стратегия
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
                await session.flush()  # Получить db_signal.id до вызова open_position

                # Открываем позицию (если торговля активна)
                status = "disabled"
                if self._positions:
                    _trade, status = await self._positions.open_position(
                        session, sig, signal_id=db_signal.id
                    )

                # Сигнал в Telegram — всегда, с реальной причиной
                if self._notifier:
                    await self._notifier.send_signal(sig, status=status)

        # 3. Аналитика — price surge детектор (только сигналы)
        if self._ps_processor:
            await self._ps_processor.process_and_notify(session)

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
