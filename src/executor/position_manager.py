import logging
from datetime import datetime, timezone
from typing import Callable, Coroutine

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.config import TradingConfig
from src.storage.models import Ticker, Trade

logger = logging.getLogger(__name__)


class PositionManager:
    """Управление виртуальными позициями: вход, TP/SL, уведомления."""

    def __init__(
        self,
        config: TradingConfig,
        send_message: Callable[[str], Coroutine] | None = None,
    ):
        self.config = config
        self._send_message = send_message

    # ------------------------------------------------------------------
    # Opening
    # ------------------------------------------------------------------

    async def open_position(
        self, session: AsyncSession, signal: Signal
    ) -> Trade | None:
        """Открыть позицию по сигналу, если есть свободные слоты."""

        # Проверка лимита
        open_count = await self._count_open(session)
        if open_count >= self.config.max_positions:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: {open_count}/{self.config.max_positions} позиций открыто"
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

        trade = Trade(
            signal_id=None,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=datetime.now(tz=timezone.utc),
            status="open",
        )
        session.add(trade)

        # Нотификация
        tp_price = self._tp_price(entry_price, signal.direction)
        sl_price = self._sl_price(entry_price, signal.direction)
        await self._notify(
            f"📈 <b>Открыта позиция</b> {signal.direction.upper()}\n"
            f"Монета: {signal.symbol}\n"
            f"Вход: ${entry_price:.6f}\n"
            f"Объём: {quantity:.2f}\n"
            f"TP: ${tp_price:.6f} (+{self.config.take_profit_pct}%)\n"
            f"SL: ${sl_price:.6f} (-{self.config.stop_loss_pct}%)"
        )

        logger.info(
            f"Позиция открыта: {signal.symbol} {signal.direction} @ {entry_price:.6f} "
            f"qty={quantity:.2f}"
        )
        return trade

    # ------------------------------------------------------------------
    # Monitoring
    # ------------------------------------------------------------------

    async def update_positions(self, session: AsyncSession) -> list[Trade]:
        """Проверить все открытые позиции на TP/SL. Возвращает закрытые."""
        stmt = (
            select(Trade)
            .where(Trade.status == "open")
            .order_by(Trade.entry_time)
        )
        result = await session.execute(stmt)
        positions = result.scalars().all()

        closed = []
        for pos in positions:
            current_price = await self._get_current_price(session, pos.symbol)
            if current_price is None:
                continue

            tp = self._tp_price(pos.entry_price, pos.direction)
            sl = self._sl_price(pos.entry_price, pos.direction)

            exit_reason = None
            if pos.direction == "long":
                if current_price >= tp:
                    exit_reason = "tp"
                elif current_price <= sl:
                    exit_reason = "sl"
            else:  # short
                if current_price <= tp:
                    exit_reason = "tp"
                elif current_price >= sl:
                    exit_reason = "sl"

            if exit_reason:
                await self._close_position(pos, current_price, exit_reason)
                closed.append(pos)

        return closed

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    def _tp_price(self, entry: float, direction: str) -> float:
        mult = 1 + self.config.take_profit_pct / 100
        return entry * mult if direction == "long" else entry / mult

    def _sl_price(self, entry: float, direction: str) -> float:
        mult = 1 - self.config.stop_loss_pct / 100
        return entry * mult if direction == "long" else entry / mult

    # ------------------------------------------------------------------
    # DB queries
    # ------------------------------------------------------------------

    async def _count_open(self, session: AsyncSession) -> int:
        from sqlalchemy import func

        stmt = select(func.count()).select_from(Trade).where(Trade.status == "open")
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
        """Последняя цена из тикера."""
        stmt = (
            select(Ticker.last)
            .where(Ticker.symbol == symbol)
            .order_by(desc(Ticker.timestamp))
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.first()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Closing
    # ------------------------------------------------------------------

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

        if reason == "tp":
            emoji, label = "✅", "Тейк-профит"
        else:
            emoji, label = "🛑", "Стоп-лосс"

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
