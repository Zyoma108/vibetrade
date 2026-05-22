import asyncio
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
        self._banned_symbols: set[str] = set()  # монеты с ошибками торговли

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
                    tp_sl_set=True,  # на бирже уже есть TP/SL, не надо выставлять повторно
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
    ) -> tuple[Trade | None, str]:
        """Открыть позицию по сигналу.
        Возвращает (trade, status): status = 'opened' | 'limit' | 'duplicate' |
        'cooldown' | 'no_price' | 'error'."""

        # Проверка лимита
        open_count = await self._count_open(session)
        if open_count >= self.config.max_positions:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"{open_count}/{self.config.max_positions} позиций открыто"
            )
            return None, "limit"

        # Проверка — монета в чёрном списке (ошибки торговли)
        if signal.symbol in self._banned_symbols:
            logger.info(f"Сигнал {signal.symbol} пропущен: монета в чёрном списке")
            return None, "error"

        # Проверка — нет ли уже позиции по этой монете
        if await self._has_position(session, signal.symbol):
            logger.info(f"Сигнал {signal.symbol} пропущен: уже есть позиция")
            return None, "duplicate"

        # Проверка кулдауна после TP/SL (сутки)
        if await self._in_cooldown(session, signal.symbol):
            logger.info(f"Сигнал {signal.symbol} пропущен: кулдаун после закрытия")
            return None, "cooldown"

        # Цена входа из последнего тикера
        entry_price = await self._get_current_price(session, signal.symbol)
        if entry_price is None or entry_price <= 0:
            logger.warning(f"Нет цены для {signal.symbol}, позиция не открыта")
            return None, "no_price"

        quantity = self.config.position_size_usdt / entry_price
        tp_price = self._tp_price(entry_price, signal.direction)
        sl_price = self._sl_price(entry_price, signal.direction)
        tp_sl_ok = True  # virtual всегда True

        # Реальный ордер на бирже
        if self.is_real:
            try:
                lev = int(self.config.leverage)
                if lev > 1:
                    try:
                        await self._connector.set_leverage(  # type: ignore[union-attr]
                            signal.symbol, lev
                        )
                    except Exception as e:
                        logger.warning(
                            f"Не удалось выставить плечо {lev}x для {signal.symbol}: {e}"
                        )

                # 1. Открываем позицию рыночным ордером
                order = await self._connector.create_market_order(  # type: ignore[union-attr]
                    symbol=signal.symbol,
                    side="buy",
                    amount=quantity,
                )

                # 2. Ждём исполнения ордера (бирже нужно время)
                await asyncio.sleep(2)

                # 3. Фактическая цена исполнения — по ней считаем TP/SL
                fill_price = order.get("fill_price") or entry_price
                if fill_price != entry_price:
                    logger.info(
                        f"Цена изменилась: тикер={entry_price:.6f} → "
                        f"факт={fill_price:.6f}"
                    )
                entry_price = fill_price
                tp_price = self._tp_price(entry_price, signal.direction)
                sl_price = self._sl_price(entry_price, signal.direction)

                # 4. Выставляем TP/SL от фактической цены
                tp_sl_ok = False
                try:
                    await self._connector.set_tpsl(  # type: ignore[union-attr]
                        symbol=signal.symbol,
                        side="buy",
                        amount=quantity,
                        tp_price=tp_price,
                        sl_price=sl_price,
                    )
                    tp_sl_ok = True
                except Exception as e:
                    err = str(e)
                    # Цена уже ушла ниже SL — аварийно закрываем позицию
                    if "lower than" in err.lower() or "higher than" in err.lower():
                        logger.error(
                            f"Цена ушла за SL для {signal.symbol}, "
                            f"аварийно закрываю позицию: {e}"
                        )
                        try:
                            await self._connector.close_position(  # type: ignore[union-attr]
                                signal.symbol
                            )
                        except Exception:
                            logger.exception(f"Не удалось аварийно закрыть {signal.symbol}")
                        await self._notify(
                            f"🆘 <b>Аварийное закрытие</b>\n"
                            f"Монета: {signal.symbol}\n"
                            f"Цена ушла за SL до его установки"
                        )
                        return None, "error"
                    else:
                        logger.warning(
                            f"TP/SL для {signal.symbol} "
                            f"будут выставлены в следующем цикле: {e}"
                        )
            except Exception as e:
                err = str(e)
                # ByBit требует подписать соглашение — пропускаем без шума
                if "sign the required agreement" in err or "110126" in err:
                    self._banned_symbols.add(signal.symbol)
                    logger.info(
                        f"ByBit не даёт торговать {signal.symbol}: "
                        f"нужно подписать соглашение на сайте (добавлен в чёрный список)"
                    )
                    return None, "error"
                logger.exception(f"Не удалось создать ордер для {signal.symbol}")
                return None, "error"

        # Запись в БД
        trade = Trade(
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=datetime.now(tz=timezone.utc),
            status="open",
            tp_sl_set=tp_sl_ok,
        )
        session.add(trade)

        # Нотификация
        mode_label = "REAL" if self.is_real else "VIRTUAL"
        margin = self.config.position_size_usdt / self.config.leverage
        await self._notify(
            f"📈 <b>Открыта позиция [{mode_label}]</b> {signal.direction.upper()}\n"
            f"Монета: {signal.symbol}\n"
            f"Вход: ${entry_price:.6f}\n"
            f"Объём: ${self.config.position_size_usdt:.0f} (маржа ${margin:.0f} на {self.config.leverage}x)\n"
            f"TP: ${tp_price:.6f} (+{self.config.take_profit_pct}%)\n"
            f"SL: ${sl_price:.6f} (-{self.config.stop_loss_pct}%)"
        )

        logger.info(
            f"Позиция открыта [{mode_label}]: {signal.symbol} @ {entry_price:.6f} "
            f"qty={quantity:.2f}"
        )
        return trade, "opened"

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
            # --- Real: повторно выставить TP/SL если не получилось ---
            if self.is_real and not pos.tp_sl_set:
                try:
                    await asyncio.sleep(1)
                    await self._connector.set_tpsl(  # type: ignore[union-attr]
                        symbol=pos.symbol,
                        side="buy",
                        amount=pos.quantity,
                        tp_price=self._tp_price(pos.entry_price, pos.direction),
                        sl_price=self._sl_price(pos.entry_price, pos.direction),
                    )
                    pos.tp_sl_set = True
                    session.add(pos)
                    logger.info(f"TP/SL повторно выставлены для {pos.symbol}")
                except Exception as e:
                    err = str(e)
                    if "lower than" in err.lower() or "higher than" in err.lower():
                        logger.error(
                            f"Цена ушла за SL для {pos.symbol}, "
                            f"аварийно закрываю позицию: {e}"
                        )
                        try:
                            await self._connector.close_position(  # type: ignore[union-attr]
                                pos.symbol
                            )
                        except Exception:
                            logger.exception(f"Не удалось аварийно закрыть {pos.symbol}")
                        current_price = await self._get_current_price(session, pos.symbol)
                        await self._close_position(
                            pos, current_price or pos.entry_price, "sl"
                        )
                        closed.append(pos)
                        continue
                    else:
                        logger.warning(f"Повторная установка TP/SL для {pos.symbol}: {e}")

            current_price = await self._get_current_price(session, pos.symbol)

            # --- Real: позиция уже закрыта на бирже (TP/SL) ---
            if self.is_real and pos.symbol not in ex_symbols:
                exit_price = current_price or pos.entry_price
                # Пытаемся получить фактическую цену выхода
                try:
                    last_trade = await self._connector.fetch_last_trade(  # type: ignore[union-attr]
                        pos.symbol, pos.entry_time
                    )
                    if last_trade:
                        exit_price = last_trade["price"]
                except Exception:
                    pass
                await self._close_position(pos, exit_price, "tp_sl_exchange")
                closed.append(pos)
                continue

            # --- Перевод стопа в б/у на полпути (без частичной фиксации) ---
            if (
                self.config.breakeven_at_halfway
                and not pos.partial_closed
                and current_price
            ):
                tp = self._tp_price(pos.entry_price, pos.direction)
                trigger = pos.entry_price + (tp - pos.entry_price) * (
                    self.config.partial_close_pct / 100
                )
                if (pos.direction == "long" and current_price >= trigger) or (
                    pos.direction == "short" and current_price <= trigger
                ):
                    pos.partial_closed = True  # флаг «б/у уже переведён»
                    session.add(pos)
                    if self.is_real:
                        try:
                            await self._connector.set_tpsl(  # type: ignore[union-attr]
                                symbol=pos.symbol,
                                side="buy" if pos.direction == "long" else "sell",
                                amount=pos.quantity,
                                tp_price=tp,
                                sl_price=pos.entry_price,
                            )
                        except Exception as e:
                            logger.warning(f"Не удалось перевести стоп в б/у для {pos.symbol}: {e}")
                    await self._notify(
                        f"🔒 <b>Стоп в безубыток</b> {pos.direction.upper()}\n"
                        f"Монета: {pos.symbol}\n"
                        f"Цена: ${current_price:.6f} → SL на вход ${pos.entry_price:.6f}"
                    )
                    logger.info(f"Стоп в б/у: {pos.symbol} @ {current_price:.6f}")
                    continue

            # --- Частичное закрытие на полпути к TP (только живая позиция) ---
            if (
                self.config.partial_close_enabled
                and not pos.partial_closed
                and current_price
            ):
                tp = self._tp_price(pos.entry_price, pos.direction)
                trigger = pos.entry_price + (tp - pos.entry_price) * (
                    self.config.partial_close_pct / 100
                )
                if (pos.direction == "long" and current_price >= trigger) or (
                    pos.direction == "short" and current_price <= trigger
                ):
                    close_qty = pos.quantity / 2
                    if self.is_real:
                        try:
                            await self._connector._call(  # type: ignore[union-attr]
                                "create_order",
                                pos.symbol, "market",
                                "sell" if pos.direction == "long" else "buy",
                                close_qty, None,
                                {"reduceOnly": True},
                            )
                            # Получаем фактический остаток и переводим стоп в б/у
                            ex_positions = await self._connector.fetch_positions(  # type: ignore[union-attr]
                                pos.symbol
                            )
                            remaining = pos.quantity - close_qty
                            if ex_positions:
                                remaining = abs(ex_positions[0]["contracts"])
                            await self._connector.set_tpsl(  # type: ignore[union-attr]
                                symbol=pos.symbol,
                                side="buy" if pos.direction == "long" else "sell",
                                amount=remaining,
                                tp_price=tp,
                                sl_price=pos.entry_price,
                            )
                        except Exception as e:
                            logger.warning(f"Частичное закрытие {pos.symbol}: {e}")
                            continue

                    partial_pnl = (current_price - pos.entry_price) * close_qty if pos.direction == "long" else (pos.entry_price - current_price) * close_qty
                    pos.quantity -= close_qty
                    pos.partial_closed = True
                    pos.partial_pnl = (pos.partial_pnl or 0.0) + partial_pnl
                    session.add(pos)

                    pnl_pct = (current_price / pos.entry_price - 1) * 100
                    await self._notify(
                        f"🔒 <b>Частичная фиксация</b> {pos.direction.upper()}\n"
                        f"Монета: {pos.symbol}\n"
                        f"Закрыто 50% @ ${current_price:.6f}\n"
                        f"Частичный PnL: ${partial_pnl:+.2f} ({pnl_pct:+.1f}%)\n"
                        f"Стоп переведён в безубыток"
                    )
                    logger.info(f"Частичное закрытие: {pos.symbol} 50% @ {current_price:.6f}")
                    continue

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

    async def _in_cooldown(self, session: AsyncSession, symbol: str) -> bool:
        """Была ли по символу закрытая сделка за последние 24 часа."""
        from datetime import timedelta
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        stmt = (
            select(Trade)
            .where(
                Trade.symbol == symbol,
                Trade.status == "closed",
                Trade.exit_time >= cutoff,
            )
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.first() is not None

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

        # PnL оставшейся части
        if trade.direction == "long":
            remainder_pnl = (exit_price - trade.entry_price) * trade.quantity
        else:
            remainder_pnl = (trade.entry_price - exit_price) * trade.quantity

        # Суммируем с частичными закрытиями
        total_pnl = remainder_pnl + (trade.partial_pnl or 0.0)
        trade.pnl = total_pnl

        pnl_pct = (
            (exit_price / trade.entry_price - 1) * 100
            if trade.direction == "long"
            else (trade.entry_price / exit_price - 1) * 100
        )

        # Если закрыто биржей — определяем TP или SL по PnL
        if reason == "tp_sl_exchange":
            reason = "tp" if (trade.pnl or 0) > 0 else "sl"

        labels = {
            "tp": ("✅", "Тейк-профит"),
            "sl": ("🛑", "Стоп-лосс"),
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
