import logging
from datetime import datetime, timezone
from typing import Callable, Coroutine

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.config import TradingConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Ticker, Trade

logger = logging.getLogger(__name__)


class PositionManager:
    """Управление позициями: вход, TP/SL, уведомления (virtual + real)."""

    def __init__(
        self,
        config: TradingConfig,
        send_message: Callable[[str], Coroutine] | None = None,
        trading_connector: ExchangeConnector | None = None,
    ):
        self.config = config
        self._send_message = send_message
        self._connector = trading_connector  # None для virtual

    @property
    def is_real(self) -> bool:
        return self._connector is not None and self._connector.has_credentials

    # ==================================================================
    # SYNC (только real) — восстановление после перезапуска
    # ==================================================================

    async def sync_positions(self, session: AsyncSession) -> None:
        """Сверить открытые позиции в БД с биржей."""
        if not self.is_real:
            return

        try:
            exchange_positions = await self._connector.fetch_positions()  # type: ignore[union-attr]
        except Exception as e:
            logger.error(f"Не удалось получить позиции с биржи: {e}")
            logger.error(
                "Проверь:\n"
                "  1) api_key/secret скопированы без пробелов\n"
                "  2) testnet: true для ключей с testnet.bybit.com\n"
                "     testnet: false для ключей с bybit.com (включая demo-счёт)\n"
                "  3) У API-ключа есть разрешения:\n"
                "     - Account → Read\n"
                "     - Trade → Derivatives (фьючерсы)\n"
                "     (в настройках API-ключа на сайте ByBit)"
            )
            return
        ex_symbols = {p["symbol"] for p in exchange_positions}

        # Позиции в БД, открытые
        db_stmt = select(Trade).where(Trade.status == "open")
        result = await session.execute(db_stmt)
        db_positions = result.scalars().all()
        db_symbols = {t.symbol for t in db_positions}

        # 1. Есть на бирже, но нет в БД → создать запись (краш перед записью)
        for ex_pos in exchange_positions:
            if ex_pos["symbol"] not in db_symbols:
                quantity = abs(ex_pos["contracts"])
                trade = Trade(
                    symbol=ex_pos["symbol"],
                    direction=ex_pos["side"],
                    entry_price=ex_pos["entry_price"],
                    quantity=quantity,
                    entry_time=ex_pos["timestamp"],
                    status="open",
                )
                session.add(trade)
                logger.info(
                    f"Sync: восстановлена позиция {ex_pos['symbol']} "
                    f"({ex_pos['side']}) из биржи"
                )
                await self._notify(
                    f"🔄 <b>Восстановлена позиция</b>\n"
                    f"Монета: {ex_pos['symbol']}\n"
                    f"Вход: ${ex_pos['entry_price']:.6f}\n"
                    f"Объём: {quantity:.2f}"
                )

        # 2. Есть в БД, но нет на бирже → закрыта вручную или TP/SL
        for db_pos in db_positions:
            if db_pos.symbol not in ex_symbols:
                current_price = await self._get_current_price(session, db_pos.symbol)
                if current_price is None:
                    current_price = db_pos.entry_price
                db_pos.exit_price = current_price
                db_pos.exit_time = datetime.now(tz=timezone.utc)
                db_pos.status = "closed"

                if db_pos.direction == "long":
                    db_pos.pnl = (current_price - db_pos.entry_price) * db_pos.quantity
                else:
                    db_pos.pnl = (db_pos.entry_price - current_price) * db_pos.quantity

                logger.info(
                    f"Sync: позиция {db_pos.symbol} закрыта (нет на бирже)"
                )

        await session.commit()
        logger.info(
            f"Sync завершён: {len(exchange_positions)} на бирже, "
            f"{len(db_positions)} в БД"
        )

    # ==================================================================
    # OPENING
    # ==================================================================

    async def open_position(
        self, session: AsyncSession, signal: Signal
    ) -> Trade | None:
        """Открыть позицию по сигналу, если есть свободные слоты."""

        # Проверка лимита
        open_count = await self._count_open(session)
        if open_count >= self.config.max_positions:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"{open_count}/{self.config.max_positions} позиций открыто"
            )
            return None

        # Проверка — нет ли уже позиции по этой монете
        if await self._has_position(session, signal.symbol):
            logger.info(f"Сигнал {signal.symbol} пропущен: уже есть позиция")
            return None

        # Цена входа из последнего тикера
        entry_price = await self._get_current_price(session, signal.symbol)
        if entry_price is None or entry_price <= 0:
            logger.warning(f"Нет цены для {signal.symbol}, позиция не открыта")
            return None

        quantity = self.config.position_size_usdt / entry_price
        tp_price = self._tp_price(entry_price, signal.direction)
        sl_price = self._sl_price(entry_price, signal.direction)

        # Реальный ордер на бирже
        if self.is_real:
            try:
                await self._connector.create_order_with_tpsl(  # type: ignore[union-attr]
                    symbol=signal.symbol,
                    side="buy",
                    amount=quantity,
                    tp_price=tp_price,
                    sl_price=sl_price,
                )
            except Exception:
                logger.exception(f"Не удалось создать ордер для {signal.symbol}")
                return None

        # Запись в БД
        trade = Trade(
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=datetime.now(tz=timezone.utc),
            status="open",
        )
        session.add(trade)

        # Нотификация
        mode_label = "REAL" if self.is_real else "VIRTUAL"
        await self._notify(
            f"📈 <b>Открыта позиция [{mode_label}]</b> {signal.direction.upper()}\n"
            f"Монета: {signal.symbol}\n"
            f"Вход: ${entry_price:.6f}\n"
            f"Объём: {quantity:.2f}\n"
            f"TP: ${tp_price:.6f} (+{self.config.take_profit_pct}%)\n"
            f"SL: ${sl_price:.6f} (-{self.config.stop_loss_pct}%)"
        )

        logger.info(
            f"Позиция открыта [{mode_label}]: {signal.symbol} @ {entry_price:.6f} "
            f"qty={quantity:.2f}"
        )
        return trade

    # ==================================================================
    # MONITORING
    # ==================================================================

    async def update_positions(self, session: AsyncSession) -> list[Trade]:
        """Проверить открытые позиции на закрытие. Возвращает закрытые."""
        stmt = (
            select(Trade)
            .where(Trade.status == "open")
            .order_by(Trade.entry_time)
        )
        result = await session.execute(stmt)
        db_positions = result.scalars().all()
        if not db_positions:
            return []

        now = datetime.now(tz=timezone.utc)
        closed = []

        # В real-режиме — сверить с биржей
        ex_symbols: set = set()
        if self.is_real:
            try:
                ex_positions = await self._connector.fetch_positions()  # type: ignore[union-attr]
                ex_symbols = {p["symbol"] for p in ex_positions}
            except Exception:
                logger.exception("Ошибка получения позиций с биржи")
                return []

        for pos in db_positions:
            current_price = await self._get_current_price(session, pos.symbol)

            # --- Выход по времени (virtual + real) ---
            age_hours = (
                now - pos.entry_time.replace(tzinfo=timezone.utc)
            ).total_seconds() / 3600
            if age_hours >= self.config.max_hold_hours:
                if self.is_real:
                    try:
                        await self._connector.close_position(pos.symbol)  # type: ignore[union-attr]
                    except Exception:
                        logger.exception(f"Ошибка закрытия {pos.symbol} по времени")
                        continue
                exit_price = current_price or pos.entry_price
                await self._close_position(pos, exit_price, "time")
                closed.append(pos)
                continue

            # --- Real: позиция закрыта на бирже (TP/SL) ---
            if self.is_real:
                if pos.symbol not in ex_symbols:
                    exit_price = current_price or pos.entry_price
                    await self._close_position(pos, exit_price, "tp_sl_exchange")
                    closed.append(pos)
                    continue

            # --- Virtual: проверка TP/SL по цене ---
            if not self.is_real and current_price:
                tp = self._tp_price(pos.entry_price, pos.direction)
                sl = self._sl_price(pos.entry_price, pos.direction)

                if pos.direction == "long":
                    if current_price >= tp:
                        await self._close_position(pos, current_price, "tp")
                        closed.append(pos)
                    elif current_price <= sl:
                        await self._close_position(pos, current_price, "sl")
                        closed.append(pos)

        return closed

    # ==================================================================
    # Helpers
    # ==================================================================

    def _tp_price(self, entry: float, direction: str) -> float:
        mult = 1 + self.config.take_profit_pct / 100
        return entry * mult if direction == "long" else entry / mult

    def _sl_price(self, entry: float, direction: str) -> float:
        mult = 1 - self.config.stop_loss_pct / 100
        return entry * mult if direction == "long" else entry / mult

    async def _count_open(self, session: AsyncSession) -> int:
        from sqlalchemy import func
        stmt = (
            select(func.count()).select_from(Trade).where(Trade.status == "open")
        )
        result = await session.execute(stmt)
        return result.scalar() or 0

    async def _has_position(self, session: AsyncSession, symbol: str) -> bool:
        stmt = (
            select(Trade)
            .where(Trade.symbol == symbol, Trade.status == "open")
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.first() is not None

    async def _get_current_price(
        self, session: AsyncSession, symbol: str
    ) -> float | None:
        stmt = (
            select(Ticker.last)
            .where(Ticker.symbol == symbol)
            .order_by(desc(Ticker.timestamp))
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.first()
        return row[0] if row else None

    async def _close_position(
        self, trade: Trade, exit_price: float, reason: str
    ) -> None:
        trade.exit_price = exit_price
        trade.exit_time = datetime.now(tz=timezone.utc)
        trade.status = "closed"

        if trade.direction == "long":
            trade.pnl = (exit_price - trade.entry_price) * trade.quantity
        else:
            trade.pnl = (trade.entry_price - exit_price) * trade.quantity

        pnl_pct = (
            (exit_price / trade.entry_price - 1) * 100
            if trade.direction == "long"
            else (trade.entry_price / exit_price - 1) * 100
        )

        labels = {
            "tp": ("✅", "Тейк-профит"),
            "sl": ("🛑", "Стоп-лосс"),
            "tp_sl_exchange": ("🏦", "Закрыто биржей (TP/SL)"),
        }
        emoji, label = labels.get(reason, ("⏰", "Выход по времени"))

        await self._notify(
            f"{emoji} <b>{label}</b> {trade.direction.upper()}\n"
            f"Монета: {trade.symbol}\n"
            f"Вход: ${trade.entry_price:.6f} → Выход: ${exit_price:.6f}\n"
            f"PnL: ${trade.pnl:+.2f} ({pnl_pct:+.1f}%)"
        )

        logger.info(
            f"Позиция закрыта: {trade.symbol} {reason} "
            f"PnL=${trade.pnl:+.2f} ({pnl_pct:+.1f}%)"
        )

    async def _notify(self, text: str) -> None:
        if self._send_message:
            try:
                await self._send_message(text)
            except Exception:
                logger.exception("Ошибка отправки торгового уведомления")
