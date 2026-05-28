import asyncio
import logging
import signal
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import BaseDetector
from src.analytics.detector import SetupDetector
from src.analytics.price_surge import PriceSurgeDetector
from src.collectors.market_data import MarketDataCollector
from src.config import Settings
from src.connectors.exchange import ExchangeConnector
from src.executor.position_manager import PositionManager
from src.notifier.telegram_bot import TelegramNotifier
from src.storage.database import async_session, init_db
from src.storage.stats import trade_stats
from src.storage.models import PriceSurgeSignal, Signal as SignalModel

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
        self._detector_price_surge: BaseDetector | None = None
        self._notifier: TelegramNotifier | None = None
        self._notifier_price_surge: TelegramNotifier | None = None
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
        else:
            logger.warning("Telegram не настроен, уведомления отключены")

        # Второй Telegram-бот для strategy_price_surge
        if self.settings.telegram_price_surge and self.settings.telegram_price_surge.bot_token and self.settings.telegram_price_surge.chat_ids:
            self._notifier_price_surge = TelegramNotifier(self.settings.telegram_price_surge)
            await self._notifier_price_surge.start()
            logger.info("Telegram-бот Price Surge запущен")
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

                # Открываем позицию (если торговля активна)
                status = "disabled"
                if self._positions:
                    _trade, status = await self._positions.open_position(session, sig)

                # Сигнал в Telegram — всегда, с реальной причиной
                if self._notifier:
                    await self._notifier.send_signal(sig, status=status)

        # 3. Аналитика — price surge детектор (только сигналы)
        if self._detector_price_surge and self._notifier_price_surge:
            from datetime import timedelta as _timedelta
            from sqlalchemy import desc as _desc, func as _func
            from src.storage.models import Candle as _Candle, OpenInterest as _OI

            ps_cfg = self.settings.strategy_price_surge
            interval = ps_cfg.price_surge_minutes
            window_bars = self._detector_price_surge._window_bars
            signals_ps = await self._detector_price_surge.analyze(session)

            for sig in signals_ps:
                # Запрашиваем свечи для получения точных цен
                c_rows = (await session.execute(
                    select(_Candle.open, _Candle.close, _Candle.timestamp)
                    .where(_Candle.symbol == sig.symbol)
                    .order_by(_desc(_Candle.timestamp))
                    .limit(window_bars + 1)
                )).all()
                if len(c_rows) < window_bars + 1:
                    continue
                c_rows = list(reversed(c_rows))
                open_p = c_rows[0][0]
                close_p = c_rows[-1][1]
                change_pct = (close_p / open_p - 1) * 100 if open_p > 0 else 0

                # Сохраняем в БД
                ps_signal = PriceSurgeSignal(
                    timestamp=datetime.now(tz=timezone.utc),
                    symbol=sig.symbol,
                    change_pct=change_pct,
                    interval_minutes=interval,
                )
                session.add(ps_signal)

                # Сигналов по тикеру за сутки
                cutoff = datetime.now(tz=timezone.utc) - _timedelta(hours=24)
                day_count = await session.scalar(
                    select(_func.count()).select_from(PriceSurgeSignal).where(
                        PriceSurgeSignal.symbol == sig.symbol,
                        PriceSurgeSignal.timestamp >= cutoff,
                    )
                ) or 0

                # Рост за час
                hour_bars = max(60 // (self.settings.collectors.timeframe.rstrip('mh') or 3), 1)
                hour_rows = (await session.execute(
                    select(_Candle.open, _Candle.close)
                    .where(_Candle.symbol == sig.symbol)
                    .order_by(_desc(_Candle.timestamp))
                    .limit(hour_bars + 1)
                )).all()
                hour_change = 0.0
                if len(hour_rows) >= hour_bars + 1:
                    hour_change = (hour_rows[0][1] / hour_rows[-1][0] - 1) * 100

                # Изменение OI (последние 3 точки)
                oi_vals = (await session.execute(
                    select(_OI.value).where(_OI.symbol == sig.symbol)
                    .order_by(_desc(_OI.timestamp)).limit(3)
                )).scalars().all()
                oi_change = 0.0
                if len(oi_vals) >= 2:
                    oi_change = (oi_vals[0] / oi_vals[-1] - 1) * 100

                # Ссылка CoinGlass
                pair = sig.symbol.split("/")[0] + "USDT"
                link = f"https://www.coinglass.com/tv/Binance_{pair}"

                text = (
                    f'📈 <b><a href="{link}">{sig.symbol}</a></b> '
                    f'+{change_pct:.1f}% за {interval} мин\n\n'
                    f'Рост за {interval} мин: +{change_pct:.1f}%  |  '
                    f'${open_p:.6f} → ${close_p:.6f}\n'
                    f'Рост за 1 час: {hour_change:+.1f}%\n'
                    f'Изменение OI: {oi_change:+.1f}%\n\n'
                    f'Сигналов за сутки: {day_count}'
                )
                await self._notifier_price_surge.notify_all(text)

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
