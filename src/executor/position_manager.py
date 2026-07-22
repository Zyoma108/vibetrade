import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Coroutine

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.config import AgentConfig, TradingConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Ticker, Trade

logger = logging.getLogger(__name__)


class PositionManager:
    """Управление позициями: вход, TP/SL, уведомления (только real).

    Один экземпляр = один "пайплайн" (source='algo' или source='agent') на своём
    аккаунте биржи (trading_connector). Оба пайплайна используют одну таблицу
    trades, но все запросы/лимиты/кулдауны скоуплены по source — они не видят
    и не блокируют друг друга.

    Здесь — только общая механика (алго- и ИИ-режим). Решения, которые принимает
    ИИ-агент по своей сделке (tighten_sl/raise_tp/partial_close/reprice/...), живут
    в подклассе `AgentPositionManager` (src/executor/agent_position_manager.py) —
    чтобы менять их, не трогая этот файл и не рискуя поведением алго-режима."""

    def __init__(
        self,
        config: TradingConfig,
        send_message: Callable[[str], Coroutine] | None = None,
        trading_connector: ExchangeConnector | None = None,
        source: str = "algo",
        agent_config: AgentConfig | None = None,
    ):
        self.config = config
        self._send_message = send_message
        self._connector = trading_connector
        self.source = source  # 'algo' | 'agent'
        self._agent_config = agent_config  # только для source='agent'
        self._banned_symbols: set[str] = set()  # монеты с ошибками торговли
        self.market_regime: str = "unknown"
        self.position_size_mult: float = 1.0
        self.block_entries: bool = False  # should_block_entries() из market_context

        # Circuit Breaker: защита от серий убытков
        self._consecutive_losses: int = 0
        self._circuit_breaker_until: datetime | None = None  # полная остановка до этого времени

        # Защита от каскада ошибок по символу
        self._error_counts: dict[str, int] = {}  # symbol → кол-во ошибок подряд
        self._error_cooldown_until: dict[str, datetime] = {}  # symbol → не пытаться до

    @property
    def _has_connector(self) -> bool:
        return self._connector is not None and self._connector.has_credentials

    # ==================================================================
    # SYNC (только real) — восстановление после перезапуска
    # ==================================================================

    async def sync_positions(self, session: AsyncSession) -> None:
        """Сверить открытые позиции в БД с биржей."""
        if not self._has_connector:
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

        # Позиции в БД, открытые (только свой пайплайн — algo/agent на разных аккаунтах)
        db_stmt = select(Trade).where(Trade.status == "open", Trade.source == self.source)
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
                    source=self.source,
                    current_sl_price=None,
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

    def _check_circuit_breaker(self) -> str | None:
        """Проверить, не заблокирована ли торговля Circuit Breaker'ом.

        Returns:
            None — можно торговать
            'circuit_breaker_stop' — полная остановка
            'circuit_breaker_reduce' — размер позиции уменьшен (торгуем дальше)
        """
        if not self.config.circuit_breaker_enabled:
            return None

        now = datetime.now(tz=timezone.utc)

        # Полная остановка?
        if self._circuit_breaker_until is not None:
            if now < self._circuit_breaker_until:
                return "circuit_breaker_stop"
            # Таймер истёк — сбрасываем
            self._circuit_breaker_until = None
            self._consecutive_losses = 0
            logger.info("Circuit Breaker: таймер остановки истёк, торговля возобновлена")

        # Уменьшение размера?
        if self._consecutive_losses >= self.config.circuit_breaker_loss_streak_stop:
            # Полная остановка
            self._circuit_breaker_until = now + timedelta(
                minutes=self.config.circuit_breaker_stop_minutes
            )
            logger.warning(
                f"Circuit Breaker: {self._consecutive_losses} убытков подряд → "
                f"ПОЛНАЯ ОСТАНОВКА на {self.config.circuit_breaker_stop_minutes} мин "
                f"(до {self._circuit_breaker_until.strftime('%H:%M:%S')})"
            )
            return "circuit_breaker_stop"

        if self._consecutive_losses >= self.config.circuit_breaker_loss_streak_reduce:
            return "circuit_breaker_reduce"

        return None

    def _get_circuit_breaker_position_mult(self) -> float:
        """Множитель размера позиции от Circuit Breaker."""
        cb_status = self._check_circuit_breaker()
        if cb_status == "circuit_breaker_reduce":
            return self.config.circuit_breaker_reduce_mult_pct / 100.0
        return 1.0

    async def open_position(
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None = None,
        force_market: bool = False,
        pullback_pct_override: float | None = None,
    ) -> tuple[Trade | None, str, str | None]:
        """Открыть позицию по сигналу (guard-проверки + диспетчер способа входа).
        Возвращает (trade, status, detail): status = 'opened' | 'pending' | 'limit' |
        'duplicate' | 'cooldown' | 'no_price' | 'error' | 'circuit_breaker_stop'.
        'pending' — лимитник на вход выставлен на откате, ждёт исполнения
        (см. `pending_entry_pullback_pct`); TP/SL и partial-close выставляются
        позже, при активации в `check_pending_entries()`.
        detail — описание ошибки (только если status не 'opened'/'pending').

        `force_market`/`pullback_pct_override` — используются только AgentPositionManager
        (entry-agent сам выбирает способ входа); алго-путь их никогда не передаёт, поэтому
        поведение по умолчанию не меняется."""

        # Проверка рыночного режима (risk_off или cautious+ST=red)
        if self.block_entries:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"рыночный режим блокирует входы (regime={self.market_regime})"
            )
            return None, "market_block", self.market_regime

        # Circuit Breaker
        cb_status = self._check_circuit_breaker()
        if cb_status == "circuit_breaker_stop":
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"Circuit Breaker — полная остановка "
                f"({self._consecutive_losses} убытков подряд)"
            )
            return None, "circuit_breaker_stop", None

        # Проверка лимита (учитывает открытые и pending-позиции)
        open_count = await self._count_open(session)
        if open_count >= self.config.max_positions:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"{open_count}/{self.config.max_positions} позиций открыто"
            )
            return None, "limit", f"max_positions={self.config.max_positions}"

        # Проверка — кулдаун после серии ошибок по символу (защита от каскада)
        cooldown_until = self._error_cooldown_until.get(signal.symbol)
        if cooldown_until is not None and datetime.now(tz=timezone.utc) < cooldown_until:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"кулдаун после {self._error_counts.get(signal.symbol, 0)} ошибок "
                f"(до {cooldown_until.strftime('%H:%M')})"
            )
            return None, "error", f"error_cooldown:{self._error_counts.get(signal.symbol, 0)}"

        # Проверка — монета в чёрном списке (ошибки торговли)
        if signal.symbol in self._banned_symbols:
            logger.info(f"Сигнал {signal.symbol} пропущен: монета в чёрном списке")
            return None, "error", "banned_symbol"

        # Проверка — нет ли уже позиции (открытой или pending) по этой монете
        if await self._has_position(session, signal.symbol):
            logger.info(f"Сигнал {signal.symbol} пропущен: уже есть позиция")
            return None, "duplicate", None

        # Проверка кулдауна после TP/SL (сутки)
        if await self._in_cooldown(session, signal.symbol):
            logger.info(f"Сигнал {signal.symbol} пропущен: кулдаун после закрытия")
            return None, "cooldown", None

        # Референсная цена (последний тикер)
        reference_price = await self._get_current_price(session, signal.symbol)
        if reference_price is None or reference_price <= 0:
            logger.warning(f"Нет цены для {signal.symbol}, позиция не открыта")
            return None, "no_price", None

        # Бюджет риска: % от депозита с биржи
        try:
            balance = await self._connector.fetch_balance()  # type: ignore[union-attr]
            total = float(balance.get("total", balance.get("free", 0)))
            if total <= 0:
                logger.warning("Баланс депозита = 0, позиция не открыта")
                return None, "error", f"zero_balance: total={total}"
        except Exception as e:
            logger.warning(f"Не удалось получить баланс: {e}")
            return None, "error", f"balance_fetch: {e}"

        # Применяем множители рыночного режима и Circuit Breaker к бюджету риска
        cb_mult = self._get_circuit_breaker_position_mult()
        risk_budget = total * (self.config.risk_per_trade_pct / 100) * self.position_size_mult * cb_mult
        if cb_mult < 1.0:
            logger.info(
                f"Circuit Breaker: размер позиции {signal.symbol} уменьшен "
                f"до {cb_mult*100:.0f}% ({self._consecutive_losses} убытков подряд)"
            )

        try:
            lev = int(self.config.leverage)
            if lev > 1:
                await self._connector.set_leverage(signal.symbol, lev)  # type: ignore[union-attr]
        except Exception as e:
            logger.warning(f"Не удалось выставить плечо для {signal.symbol}: {e}")

        effective_pullback_pct = (
            pullback_pct_override
            if pullback_pct_override is not None
            else self.config.pending_entry_pullback_pct
        )
        if not force_market and effective_pullback_pct > 0:
            return await self._place_pending_entry(
                session, signal, signal_id, reference_price, risk_budget,
                pullback_pct_override=pullback_pct_override,
            )
        return await self._place_market_entry(
            session, signal, signal_id, reference_price, risk_budget
        )

    async def _place_market_entry(
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None,
        entry_price: float,
        risk_budget: float,
    ) -> tuple[Trade | None, str, str | None]:
        """Немедленный вход market-ордером (pending_entry_pullback_pct == 0)."""
        sl_distance = entry_price * (self.config.stop_loss_pct / 100)
        tp_distance = sl_distance * self.config.risk_reward_ratio
        quantity = risk_budget / sl_distance

        tp_price = entry_price + tp_distance
        sl_price = entry_price - sl_distance

        actual_size = quantity * entry_price
        tp_pct = (tp_distance / entry_price * 100) if entry_price > 0 else 0
        sl_pct = (sl_distance / entry_price * 100) if entry_price > 0 else 0
        logger.info(
            f"Позиция {signal.symbol}: SL={sl_distance:.6f} ({sl_pct:.1f}%), "
            f"TP={tp_distance:.6f} ({tp_pct:.1f}%), "
            f"qty={quantity:.2f}, размер=${actual_size:.0f} "
            f"(риск=${risk_budget:.2f})"
        )

        tp_sl_ok = False

        # Ордер на бирже
        try:
            # 1. Открываем позицию рыночным ордером
            await self._connector.create_market_order(  # type: ignore[union-attr]
                symbol=signal.symbol,
                side="buy",
                amount=quantity,
            )

            # 2. Ждём исполнения и получаем фактическую цену с биржи
            await asyncio.sleep(2)
            try:
                ex_positions = await self._connector.fetch_positions(  # type: ignore[union-attr]
                    signal.symbol
                )
                if ex_positions and ex_positions[0].get("entry_price"):
                    fill_price = ex_positions[0]["entry_price"]
                    if fill_price != entry_price:
                        logger.info(
                            f"Цена изменилась: тикер={entry_price:.6f} → "
                            f"биржа={fill_price:.6f}"
                        )
                    entry_price = fill_price
            except Exception as e:
                logger.warning(f"Не удалось получить цену входа с биржи: {e}")

            tp_price = self._tp_price(entry_price)
            sl_price = self._sl_price(entry_price)
            logger.info(
                f"TP/SL пересчитаны от цены заполнения: "
                f"TP=${tp_price:.6f}, SL=${sl_price:.6f}"
            )

            # 3. Выставляем TP/SL от фактической цены
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
                    return None, "error", f"emergency_close_sl_breach: {e}"
                else:
                    logger.warning(
                        f"TP/SL для {signal.symbol} "
                        f"будут выставлены в следующем цикле: {e}"
                    )

            # 4. Частичная фиксация — лимитный ордер на 50% позиции
            # Выставляется сразу при открытии, не зависит от цикла опроса.
            if tp_sl_ok:
                try:
                    partial_trigger = self._partial_trigger_price(entry_price, tp_price)
                    partial_qty = quantity / 2
                    await self._connector.place_reduce_only_limit(  # type: ignore[union-attr]
                        symbol=signal.symbol,
                        side="buy",
                        amount=partial_qty,
                        price=partial_trigger,
                    )
                    logger.info(
                        f"Лимитник частичной фиксации {signal.symbol}: "
                        f"{partial_qty:.2f} контрактов @ {partial_trigger:.6f} "
                        f"({self.config.partial_close_pct:.0f}% пути до TP)"
                    )
                except Exception:
                    # Не критично — update_positions проверит частичную
                    # фиксацию по цене как fallback.
                    logger.warning(
                        f"Не удалось выставить лимитник частичной "
                        f"фиксации для {signal.symbol}, будет проверка по циклу"
                    )

        except Exception as e:
            err = str(e)
            # ByBit требует подписать соглашение — пропускаем без шума
            if "sign the required agreement" in err or "110126" in err:
                self._banned_symbols.add(signal.symbol)
                self._track_error(signal.symbol)
                logger.info(
                    f"ByBit не даёт торговать {signal.symbol}: "
                    f"нужно подписать соглашение на сайте (добавлен в чёрный список)"
                )
                return None, "error", f"bybit_agreement: {err[:120]}"
            self._track_error(signal.symbol)
            logger.exception(f"Не удалось создать ордер для {signal.symbol}")
            return None, "error", f"order: {err[:120]}"

        # Запись в БД
        actual_size = quantity * entry_price
        entry_fee = self._fee(actual_size, taker=True)
        trade = Trade(
            signal_id=signal_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            quantity=quantity,
            entry_time=datetime.now(tz=timezone.utc),
            status="open",
            tp_sl_set=tp_sl_ok,
            fee=entry_fee,
            source=self.source,
            current_sl_price=sl_price if tp_sl_ok else None,
        )
        session.add(trade)

        # Нотификация
        margin = actual_size / self.config.leverage
        tp_pct = (tp_price / entry_price - 1) * 100
        sl_pct = (1 - sl_price / entry_price) * 100
        await self._notify(
            f"📈 <b>Открыта позиция</b> {signal.direction.upper()}\n"
            f"Монета: {signal.symbol}\n"
            f"Вход: ${entry_price:.6f}\n"
            f"Объём: ${actual_size:.0f} (маржа ${margin:.0f} на {self.config.leverage}x)\n"
            f"TP: ${tp_price:.6f} (+{tp_pct:.1f}% | 1:{self.config.risk_reward_ratio})\n"
            f"SL: ${sl_price:.6f} (-{sl_pct:.1f}%)\n"
            f"Комиссия входа: ${entry_fee:.4f}"
        )

        logger.info(
            f"Позиция открыта: {signal.symbol} @ {entry_price:.6f} "
            f"qty={quantity:.2f}"
        )
        self._reset_errors(signal.symbol)
        return trade, "opened", None

    async def _place_pending_entry(
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None,
        reference_price: float,
        risk_budget: float,
        pullback_pct_override: float | None = None,
    ) -> tuple[Trade | None, str, str | None]:
        """Вход лимитным ордером на откате от цены сигнала (решает проблему
        покупки на пике пампа). TP/SL и partial-close выставляются позже,
        при исполнении лимитника — см. `check_pending_entries()`.
        `pullback_pct_override` — свой откат вместо конфигового (см. AgentPositionManager)."""
        pullback_pct = (
            pullback_pct_override
            if pullback_pct_override is not None
            else self.config.pending_entry_pullback_pct
        )
        limit_price = reference_price * (1 - pullback_pct / 100)
        sl_distance = limit_price * (self.config.stop_loss_pct / 100)
        quantity = risk_budget / sl_distance

        logger.info(
            f"Pending-вход {signal.symbol}: сигнал=${reference_price:.6f} → "
            f"лимит=${limit_price:.6f} (откат {pullback_pct}%), "
            f"qty={quantity:.2f}, размер=${quantity * limit_price:.0f}"
        )

        try:
            await self._connector.create_limit_order(  # type: ignore[union-attr]
                symbol=signal.symbol, side="buy", amount=quantity, price=limit_price,
            )
        except Exception as e:
            err = str(e)
            if "sign the required agreement" in err or "110126" in err:
                self._banned_symbols.add(signal.symbol)
                self._track_error(signal.symbol)
                logger.info(
                    f"ByBit не даёт торговать {signal.symbol}: "
                    f"нужно подписать соглашение на сайте (добавлен в чёрный список)"
                )
                return None, "error", f"bybit_agreement: {err[:120]}"
            self._track_error(signal.symbol)
            logger.exception(f"Не удалось выставить лимитник входа для {signal.symbol}")
            return None, "error", f"pending_order: {err[:120]}"

        expires_at = datetime.now(tz=timezone.utc) + timedelta(
            minutes=self.config.pending_entry_timeout_minutes
        )
        trade = Trade(
            signal_id=signal_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=limit_price,
            quantity=quantity,
            entry_time=datetime.now(tz=timezone.utc),
            status="pending",
            pending_expires_at=expires_at,
            source=self.source,
        )
        session.add(trade)

        await self._notify(
            f"⏳ <b>Лимитник на вход выставлен</b> {signal.direction.upper()}\n"
            f"Монета: {signal.symbol}\n"
            f"Сигнал: ${reference_price:.6f} → Лимит: ${limit_price:.6f} "
            f"(откат {pullback_pct}%)\n"
            f"Истекает через {self.config.pending_entry_timeout_minutes:.0f} мин"
        )
        logger.info(f"Pending-вход выставлен: {signal.symbol} @ {limit_price:.6f}")
        self._reset_errors(signal.symbol)
        return trade, "pending", None

    # ==================================================================
    # PENDING ENTRIES — лимитники на вход, ожидающие отката
    # ==================================================================

    async def check_pending_entries(self, session: AsyncSession) -> list[Trade]:
        """Проверить лимитники на вход: исполнились или истёк таймаут.
        Возвращает список позиций, активированных в этом вызове (для логирования)."""
        stmt = (
            select(Trade)
            .where(Trade.status == "pending", Trade.source == self.source)
            .order_by(Trade.entry_time)
        )
        result = await session.execute(stmt)
        pending = result.scalars().all()
        if not pending:
            return []

        now = datetime.now(tz=timezone.utc)
        activated = []
        for pos in pending:
            try:
                ex_positions = await self._connector.fetch_positions(pos.symbol)  # type: ignore[union-attr]
            except Exception:
                logger.warning(f"Ошибка проверки pending-входа для {pos.symbol}")
                continue

            if ex_positions:
                await self._activate_pending_entry(pos, ex_positions[0])
                activated.append(pos)
                continue

            expires_at = pos.pending_expires_at
            if expires_at and now >= expires_at.replace(tzinfo=timezone.utc):
                await self._expire_pending_entry(pos)

        return activated

    async def _activate_pending_entry(self, pos: Trade, ex_position: dict) -> None:
        """Лимитник на вход исполнился — перевести в open, выставить TP/SL
        и лимитник частичной фиксации (то же самое, что делает market-путь
        сразу при открытии)."""
        fill_price = ex_position.get("entry_price") or pos.entry_price
        pos.entry_price = fill_price
        pos.entry_time = datetime.now(tz=timezone.utc)
        pos.status = "open"

        # Лимитник на вход исполнился как maker (резидентный ордер в стакане)
        tp_price, sl_price = await self._setup_tp_sl_and_partial(pos, fill_price, is_maker=True)

        tp_pct = (tp_price / fill_price - 1) * 100
        sl_pct = (1 - sl_price / fill_price) * 100
        await self._notify(
            f"✅ <b>Лимитник на вход исполнен</b> {pos.direction.upper()}\n"
            f"Монета: {pos.symbol}\n"
            f"Вход: ${fill_price:.6f}\n"
            f"TP: ${tp_price:.6f} (+{tp_pct:.1f}%) | SL: ${sl_price:.6f} (-{sl_pct:.1f}%)"
        )
        logger.info(f"Pending-вход исполнен: {pos.symbol} @ {fill_price:.6f}")
        self._reset_errors(pos.symbol)

    async def _setup_tp_sl_and_partial(
        self, pos: Trade, fill_price: float, is_maker: bool
    ) -> tuple[float, float]:
        """Общая часть активации позиции после исполнения входа (лимитником или
        market-ордером): комиссия, TP/SL, лимитник частичной фиксации. Возвращает
        (tp_price, sl_price). Используется `_activate_pending_entry` (механическое
        исполнение лимитника, ОБА источника) и `AgentPositionManager.
        apply_agent_convert_to_market` (агент перевёл pending в market)."""
        pos.fee = (pos.fee or 0.0) + self._fee(pos.quantity * fill_price, taker=not is_maker)

        tp_price = self._tp_price(fill_price)
        sl_price = self._sl_price(fill_price)
        tp_sl_ok = False
        try:
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy",
                amount=pos.quantity,
                tp_price=tp_price,
                sl_price=sl_price,
            )
            tp_sl_ok = True
        except Exception as e:
            logger.warning(f"TP/SL для {pos.symbol} будут выставлены в следующем цикле: {e}")
        pos.tp_sl_set = tp_sl_ok
        pos.current_sl_price = sl_price if tp_sl_ok else None

        if tp_sl_ok:
            try:
                partial_trigger = self._partial_trigger_price(fill_price, tp_price)
                await self._connector.place_reduce_only_limit(  # type: ignore[union-attr]
                    symbol=pos.symbol,
                    side="buy",
                    amount=pos.quantity / 2,
                    price=partial_trigger,
                )
            except Exception:
                logger.warning(
                    f"Не удалось выставить лимитник частичной фиксации для {pos.symbol}"
                )

        return tp_price, sl_price

    async def _expire_pending_entry(self, pos: Trade) -> None:
        """Лимитник на вход не исполнился за отведённое время — снять."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Не удалось отменить лимитник входа для {pos.symbol}")
        pos.status = "expired"
        await self._notify(
            f"⌛ <b>Лимитник на вход истёк</b>\n"
            f"Монета: {pos.symbol}\n"
            f"Цена не откатилась до ${pos.entry_price:.6f} за "
            f"{self.config.pending_entry_timeout_minutes:.0f} мин — сетап устарел"
        )
        logger.info(f"Pending-вход истёк: {pos.symbol}")

    # ==================================================================
    # MONITORING
    # ==================================================================

    async def update_positions(self, session: AsyncSession) -> list[Trade]:
        """Проверить открытые позиции на закрытие. Возвращает закрытые.

        Для каждой позиции последовательно проверяются (первая сработавшая
        стадия закрывает итерацию): повторная установка TP/SL → закрытие
        биржей (TP/SL) → частичная фиксация лимитником → частичная фиксация
        fallback'ом → выход по времени.
        """
        stmt = (
            select(Trade)
            .where(Trade.status == "open", Trade.source == self.source)
            .order_by(Trade.entry_time)
        )
        result = await session.execute(stmt)
        db_positions = result.scalars().all()
        if not db_positions:
            return []

        now = datetime.now(tz=timezone.utc)
        closed: list[Trade] = []

        # Сверить с биржей
        try:
            ex_positions = await self._connector.fetch_positions()  # type: ignore[union-attr]
            ex_symbols = {p["symbol"] for p in ex_positions}
        except Exception:
            logger.exception("Ошибка получения позиций с биржи")
            return []

        for pos in db_positions:
            if not pos.tp_sl_set:
                if await self._resync_missing_tpsl(session, pos, closed):
                    continue

            current_price = await self._get_current_price(session, pos.symbol)

            if pos.symbol not in ex_symbols:
                await self._close_from_exchange(pos, current_price, closed)
                continue

            if not pos.partial_closed and await self._check_limit_partial_fill(pos):
                continue

            if not pos.partial_closed and current_price:
                if await self._check_partial_close_fallback(pos, current_price):
                    continue

            if await self._check_time_exit(pos, now, current_price, closed):
                continue

        return closed

    # ------------------------------------------------------------------
    # update_positions — по одной стадии на метод
    # ------------------------------------------------------------------

    async def _resync_missing_tpsl(
        self, session: AsyncSession, pos: Trade, closed: list[Trade]
    ) -> bool:
        """Повторно выставить TP/SL, если не удалось при открытии.

        Возвращает True, если позиция аварийно закрыта (цена уже за SL) —
        в этом случае вызывающий цикл должен перейти к следующей позиции.
        """
        try:
            await asyncio.sleep(1)
            tp = self._tp_price(pos.entry_price)
            sl = self._sl_price(pos.entry_price)
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy",
                amount=pos.quantity,
                tp_price=tp,
                sl_price=sl,
            )
            pos.tp_sl_set = True
            pos.current_sl_price = sl
            session.add(pos)
            logger.info(f"TP/SL повторно выставлены для {pos.symbol}")
            return False
        except Exception as e:
            err = str(e)
            if "lower than" not in err.lower() and "higher than" not in err.lower():
                logger.warning(f"Повторная установка TP/SL для {pos.symbol}: {e}")
                return False

            logger.error(
                f"Цена ушла за SL для {pos.symbol}, аварийно закрываю позицию: {e}"
            )
            try:
                await self._connector.close_position(pos.symbol)  # type: ignore[union-attr]
            except Exception:
                logger.exception(f"Не удалось аварийно закрыть {pos.symbol}")
            current_price = await self._get_current_price(session, pos.symbol)
            await self._close_position(pos, current_price or pos.entry_price, "sl")
            closed.append(pos)
            return True

    async def _close_from_exchange(
        self, pos: Trade, current_price: float | None, closed: list[Trade]
    ) -> None:
        """Позиция уже закрыта на бирже (сработал TP/SL) — синхронизировать в БД."""
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

    def _partial_trigger_price(self, entry_price: float, tp_price: float) -> float:
        """Цена срабатывания частичной фиксации (% пути от входа до TP)."""
        return entry_price + (tp_price - entry_price) * (
            self.config.partial_close_pct / 100
        )

    async def _check_limit_partial_fill(self, pos: Trade) -> bool:
        """Проверить исполнение лимитника частичной фиксации.

        Возвращает True, если лимитник исполнился и позиция обработана.
        """
        try:
            ex_positions = await self._connector.fetch_positions(  # type: ignore[union-attr]
                pos.symbol
            )
        except Exception:
            logger.warning(f"Ошибка проверки лимитника для {pos.symbol}")
            return False

        if not ex_positions:
            return False

        actual_contracts = abs(ex_positions[0]["contracts"])
        if actual_contracts >= pos.quantity * 0.75:
            return False

        # Позиция уменьшилась → лимитник исполнился
        trigger = self._partial_trigger_price(pos.entry_price, self._tp_price(pos.entry_price))
        close_qty = pos.quantity - actual_contracts
        partial_pnl = (
            (trigger - pos.entry_price) * close_qty
            if pos.direction == "long"
            else (pos.entry_price - trigger) * close_qty
        )
        pos.quantity = actual_contracts
        pos.partial_closed = True
        pos.partial_pnl = (pos.partial_pnl or 0.0) + partial_pnl
        # Резервный лимитный ордер исполнился как maker
        pos.fee = (pos.fee or 0.0) + self._fee(trigger * close_qty, taker=False)

        # Переводим стоп в безубыток для остатка
        try:
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=actual_contracts,
                tp_price=self._tp_price(pos.entry_price),
                sl_price=pos.entry_price,
            )
            pos.current_sl_price = pos.entry_price
        except Exception as e:
            logger.warning(f"Не удалось перевести стоп в б/у для {pos.symbol}: {e}")

        pnl_pct = (trigger / pos.entry_price - 1) * 100
        await self._notify(
            f"🔒 <b>Частичная фиксация (лимитник)</b> {pos.direction.upper()}\n"
            f"Монета: {pos.symbol}\n"
            f"Закрыто 50% @ ${trigger:.6f}\n"
            f"Частичный PnL: ${partial_pnl:+.2f} ({pnl_pct:+.1f}%)\n"
            f"Стоп переведён в безубыток"
        )
        logger.info(f"Лимитник исполнен: {pos.symbol} {close_qty:.2f} @ {trigger:.6f}")
        return True

    async def _check_partial_close_fallback(
        self, pos: Trade, current_price: float
    ) -> bool:
        """Частичное закрытие по рынку, если лимитник не был выставлен (fallback).

        Возвращает True, если позиция обработана (обработка триггера,
        неудача ордера и уже-существующий лимитник — всё считается обработкой).
        """
        trigger = self._partial_trigger_price(pos.entry_price, self._tp_price(pos.entry_price))
        triggered = (pos.direction == "long" and current_price >= trigger) or (
            pos.direction == "short" and current_price <= trigger
        )
        if not triggered:
            return False

        close_qty = pos.quantity / 2

        # Проверить, нет ли уже лимитника на бирже (после рестарта)
        has_open_orders = False
        try:
            open_orders = await self._connector._call(  # type: ignore[union-attr]
                "fetch_open_orders", pos.symbol
            )
            has_open_orders = len(open_orders) > 0
        except Exception:
            pass

        if has_open_orders:
            logger.info(
                f"Частичная фиксация {pos.symbol}: "
                f"на бирже есть открытые ордера, "
                f"пропускаем fallback (лимитник уже работает)"
            )
            return True

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
                tp_price=self._tp_price(pos.entry_price),
                sl_price=pos.entry_price,
            )
            pos.current_sl_price = pos.entry_price
        except Exception as e:
            logger.warning(f"Частичное закрытие {pos.symbol}: {e}")
            return True

        partial_pnl = (
            (current_price - pos.entry_price) * close_qty
            if pos.direction == "long"
            else (pos.entry_price - current_price) * close_qty
        )
        pos.quantity -= close_qty
        pos.partial_closed = True
        pos.partial_pnl = (pos.partial_pnl or 0.0) + partial_pnl
        # Fallback закрывает market-ордером — taker
        pos.fee = (pos.fee or 0.0) + self._fee(current_price * close_qty, taker=True)

        pnl_pct = (current_price / pos.entry_price - 1) * 100
        await self._notify(
            f"🔒 <b>Частичная фиксация</b> {pos.direction.upper()}\n"
            f"Монета: {pos.symbol}\n"
            f"Закрыто 50% @ ${current_price:.6f}\n"
            f"Частичный PnL: ${partial_pnl:+.2f} ({pnl_pct:+.1f}%)\n"
            f"Стоп переведён в безубыток"
        )
        logger.info(f"Частичное закрытие: {pos.symbol} 50% @ {current_price:.6f}")
        return True

    async def _check_time_exit(
        self,
        pos: Trade,
        now: datetime,
        current_price: float | None,
        closed: list[Trade],
    ) -> bool:
        """Закрыть позицию по истечении max_hold_hours (агент может только
        ПРОДЛИТЬ этот дедлайн через llm_hold_until, никогда не сократить).

        Возвращает True, если стадия обработана (позиция закрыта, либо
        закрытие не удалось и будет повторено в следующем цикле).
        """
        deadline = pos.entry_time.replace(tzinfo=timezone.utc) + timedelta(
            hours=self.config.max_hold_hours
        )
        if pos.llm_hold_until:
            deadline = max(deadline, pos.llm_hold_until.replace(tzinfo=timezone.utc))
        if now < deadline:
            return False

        try:
            await self._connector.close_position(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.exception(f"Ошибка закрытия {pos.symbol} по времени")
            return True

        exit_price = current_price or pos.entry_price
        await self._close_position(pos, exit_price, "time")
        closed.append(pos)
        return True

    # ==================================================================
    # Helpers
    # ==================================================================

    def _tp_price(self, entry: float) -> float:
        """TP: entry + stop_loss_pct% × risk_reward_ratio."""
        sl_distance = entry * (self.config.stop_loss_pct / 100)
        return entry + sl_distance * self.config.risk_reward_ratio

    def _sl_price(self, entry: float) -> float:
        """SL: entry − stop_loss_pct%."""
        return entry * (1 - self.config.stop_loss_pct / 100)

    def _fee(self, notional: float, taker: bool) -> float:
        """Комиссия биржи за одну "ногу" сделки.
        taker=True — market-ордер (вход, TP/SL-триггер, time-exit, fallback partial).
        taker=False — резервный reduce-only лимитник (исполняется как maker)."""
        rate = self.config.taker_fee_pct if taker else self.config.maker_fee_pct
        return notional * (rate / 100)

    async def _count_open(self, session: AsyncSession) -> int:
        """Открытые позиции + pending-заявки на вход своего пайплайна (обе занимают
        "слот" max_positions). algo и agent считаются раздельно — разные аккаунты."""
        from sqlalchemy import func
        stmt = (
            select(func.count()).select_from(Trade)
            .where(Trade.status.in_(["open", "pending"]), Trade.source == self.source)
        )
        result = await session.execute(stmt)
        return result.scalar() or 0

    async def _in_cooldown(self, session: AsyncSession, symbol: str) -> bool:
        """Была ли по символу закрытая сделка своего пайплайна за последние N часов."""
        if self.config.cooldown_hours <= 0:
            return False
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=self.config.cooldown_hours)
        stmt = (
            select(Trade)
            .where(
                Trade.symbol == symbol,
                Trade.status == "closed",
                Trade.exit_time >= cutoff,
                Trade.source == self.source,
            )
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.first() is not None

    async def _has_position(self, session: AsyncSession, symbol: str) -> bool:
        """Есть ли уже открытая позиция или pending-заявка на вход по символу в своём пайплайне."""
        stmt = (
            select(Trade)
            .where(Trade.symbol == symbol, Trade.status.in_(["open", "pending"]), Trade.source == self.source)
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

        # Финальный выход — market (TP/SL/time-exit/аварийное закрытие всегда taker)
        trade.fee = (trade.fee or 0.0) + self._fee(exit_price * trade.quantity, taker=True)

        # Суммируем с частичными закрытиями, вычитаем комиссию всех "ног" сделки
        total_pnl = remainder_pnl + (trade.partial_pnl or 0.0) - (trade.fee or 0.0)
        trade.pnl = total_pnl

        pnl_pct = (
            (exit_price / trade.entry_price - 1) * 100
            if trade.direction == "long"
            else (trade.entry_price / exit_price - 1) * 100
        )

        # Если закрыто биржей — определяем TP или SL по PnL
        if reason == "tp_sl_exchange":
            reason = "tp" if (trade.pnl or 0) > 0 else "sl"

        # Circuit Breaker: обновляем счётчик убытков подряд
        if self.config.circuit_breaker_enabled:
            if (trade.pnl or 0) <= 0:
                self._consecutive_losses += 1
                logger.warning(
                    f"Circuit Breaker: {self._consecutive_losses} убытков подряд "
                    f"(PnL=${trade.pnl:+.2f} на {trade.symbol})"
                )
                if self._consecutive_losses >= self.config.circuit_breaker_loss_streak_reduce:
                    mult = self.config.circuit_breaker_reduce_mult_pct
                    logger.warning(
                        f"Circuit Breaker: размер позиций уменьшен до {mult:.0f}%"
                    )
            else:
                if self._consecutive_losses > 0:
                    logger.info(
                        f"Circuit Breaker: серия из {self._consecutive_losses} убытков "
                        f"прервана прибылью ${trade.pnl:+.2f} на {trade.symbol}"
                    )
                self._consecutive_losses = 0
                self._circuit_breaker_until = None

        labels = {
            "tp": ("✅", "Тейк-профит"),
            "sl": ("🛑", "Стоп-лосс"),
            "llm_close": ("🤖", "Закрыто ИИ-агентом"),
        }
        emoji, label = labels.get(reason, ("⏰", "Выход по времени"))

        await self._notify(
            f"{emoji} <b>{label}</b> {trade.direction.upper()}\n"
            f"Монета: {trade.symbol}\n"
            f"Вход: ${trade.entry_price:.6f} → Выход: ${exit_price:.6f}\n"
            f"PnL: ${trade.pnl:+.2f} ({pnl_pct:+.1f}%) "
            f"[комиссии: ${trade.fee or 0.0:.4f}]"
        )

        logger.info(
            f"Позиция закрыта: {trade.symbol} {reason} "
            f"PnL=${trade.pnl:+.2f} ({pnl_pct:+.1f}%) fee=${trade.fee or 0.0:.4f}"
        )

    async def _notify(self, text: str) -> None:
        if self._send_message:
            try:
                await self._send_message(text)
            except Exception:
                logger.exception("Ошибка отправки торгового уведомления")

    # ------------------------------------------------------------------
    # Error cascade protection
    # ------------------------------------------------------------------

    def _track_error(self, symbol: str) -> None:
        """Зафиксировать ошибку открытия позиции по символу.
        После 3 ошибок подряд — кулдаун 4 часа (защита от каскада)."""
        count = self._error_counts.get(symbol, 0) + 1
        self._error_counts[symbol] = count
        if count >= 3:
            cooldown_hours = 4
            self._error_cooldown_until[symbol] = (
                datetime.now(tz=timezone.utc)
                + timedelta(hours=cooldown_hours)
            )
            logger.warning(
                f"Error cascade: {symbol} — {count} ошибок подряд, "
                f"кулдаун на {cooldown_hours}ч"
            )

    def _reset_errors(self, symbol: str) -> None:
        """Сбросить счётчик ошибок после успешной сделки."""
        self._error_counts.pop(symbol, None)
        self._error_cooldown_until.pop(symbol, None)
