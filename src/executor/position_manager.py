import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Coroutine

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.config import TradingConfig
from src.connectors.exchange import ExchangeConnector
from src.executor.guards import SOURCE, TradingGuards
from src.storage.models import Ticker, Trade

logger = logging.getLogger(__name__)


# ByBit отказывает в торговле «особыми» контрактами (токенизированные акции,
# товарные фьючерсы) двумя разными кодами, и до 01.09.2026 ловился только один.
# Аудит боевой БД за 27.08-01.09: XAG/USDT отдавал 110123 («agree to the Trading
# Terms»), под бан не попадал и продолжал жечь сигналы циклами по три ошибки и
# четыре часа кулдауна — в отличие от CRCL и NVDA, которые отдавали 110126 и
# были забанены с первой ошибки. Всего на этой тройке ушло 10 сигналов из 44.
#
# Ловим класс целиком, а не перечисляем монеты: следующий такой контракт
# забанится сам. Список `strategy.exclude_coins` — вторая линия, она убирает
# уже известные символы ещё на этапе сканирования.
_AGREEMENT_ERROR_MARKERS = (
    "110126",                        # You must sign the required agreement
    "110123",                        # You must agree to the Trading Terms
    "sign the required agreement",
    "agree to the trading terms",
)


def _is_agreement_error(err: str) -> bool:
    """Отказ ByBit из-за неподписанного соглашения по контракту."""
    low = err.lower()
    return any(marker in low for marker in _AGREEMENT_ERROR_MARKERS)



class PositionManager:
    """Управление позициями: вход, TP/SL, уведомления (только real)."""

    def __init__(
        self,
        config: TradingConfig,
        trading_connector: ExchangeConnector,
        send_message: Callable[[str], Coroutine] | None = None,
    ):
        """`trading_connector` обязателен. Он был `| None` только ради удобства
        тестов, и цена этого — 26 подавлений `type: ignore[union-attr]` в боевом
        коде: в проде менеджер создаётся исключительно внутри `if mode == "real"`,
        где отсутствие ключей уже привело бы к RuntimeError выше по стеку. Тесты
        передают фейковый коннектор."""
        self.config = config
        self._send_message = send_message
        self._connector = trading_connector
        self.guards = TradingGuards(config)  # Circuit Breaker, бан-лист, кулдаун ошибок
        # Очередь уведомлений: наполняется внутри транзакции, отправляется после
        # коммита — см. _notify/flush_notifications.
        self._outbox: list[str] = []
        self.market_regime: str = "unknown"
        self.position_size_mult: float = 1.0
        self.block_entries: bool = False  # should_block_entries() из market_context



        # Алерт в Telegram при ошибках связи с биржей (истёкший/невалидный API-ключ и т.п.)
        self._exchange_error_since: datetime | None = None  # None = сейчас всё ок
        self._exchange_error_last_alert_at: datetime | None = None

    @property
    def _has_connector(self) -> bool:
        """Коннектор есть всегда, но без ключей торговать нечем (режим signal)."""
        return self._connector.has_credentials

    # ==================================================================
    # SYNC (только real) — восстановление после перезапуска
    # ==================================================================

    async def sync_positions(self, session: AsyncSession) -> None:
        """Сверить открытые позиции в БД с биржей."""
        if not self._has_connector:
            return

        try:
            exchange_positions = await self._connector.fetch_positions()
            await self._clear_exchange_error()
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
            await self._alert_exchange_error("fetch_positions (sync при старте)", e)
            return
        ex_symbols = {p["symbol"] for p in exchange_positions}

        # Позиции в БД, открытые
        db_stmt = select(Trade).where(Trade.status == "open", Trade.source == SOURCE)
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
                    source=SOURCE,
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



    async def open_position(
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None = None,
    ) -> tuple[Trade | None, str, str | None]:
        """Открыть позицию по сигналу (guard-проверки + диспетчер способа входа).
        Возвращает (trade, status, detail): status = 'opened' | 'pending' | 'limit' |
        'duplicate' | 'cooldown' | 'no_price' | 'error' | 'circuit_breaker_stop'.
        'pending' — лимитник на вход выставлен на откате, ждёт исполнения
        (см. `pending_entry_pullback_pct`); TP/SL и partial-close выставляются
        позже, при активации в `check_pending_entries()`.
        detail — описание ошибки (только если status не 'opened'/'pending')."""

        # Проверка рыночного режима (risk_off или cautious+ST=red)
        if self.block_entries:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"рыночный режим блокирует входы (regime={self.market_regime})"
            )
            return None, "market_block", self.market_regime

        # Circuit Breaker
        cb_status = self.guards.check_circuit_breaker()
        if cb_status == "circuit_breaker_stop":
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"Circuit Breaker — полная остановка "
                f"({self.guards.consecutive_losses} убытков подряд)"
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
        cooldown_until = self.guards.error_cooldown_left(signal.symbol)
        if cooldown_until is not None:
            errors = self.guards.error_count(signal.symbol)
            logger.info(
                f"Сигнал {signal.symbol} пропущен: "
                f"кулдаун после {errors} ошибок "
                f"(до {cooldown_until.strftime('%H:%M')})"
            )
            return None, "error", f"error_cooldown:{errors}"

        # Проверка — монета в чёрном списке (ошибки торговли)
        if self.guards.is_banned(signal.symbol):
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

        # Референсная цена — живая с биржи (см. _get_entry_price про то, почему
        # не сохранённый тикер)
        reference_price = await self._get_entry_price(session, signal.symbol)
        if reference_price is None or reference_price <= 0:
            logger.warning(f"Нет цены для {signal.symbol}, позиция не открыта")
            return None, "no_price", None

        # Бюджет риска: % от депозита с биржи
        try:
            balance = await self._connector.fetch_balance()
            await self._clear_exchange_error()
            total = float(balance.get("total", balance.get("free", 0)))
            if total <= 0:
                logger.warning("Баланс депозита = 0, позиция не открыта")
                return None, "error", f"zero_balance: total={total}"
        except Exception as e:
            logger.warning(f"Не удалось получить баланс: {e}")
            await self._alert_exchange_error("fetch_balance (открытие позиции)", e)
            return None, "error", f"balance_fetch: {e}"

        # Применяем множители рыночного режима и Circuit Breaker к бюджету риска
        cb_mult = self.guards.position_size_mult()
        risk_budget = total * (self.config.risk_per_trade_pct / 100) * self.position_size_mult * cb_mult
        if cb_mult < 1.0:
            logger.info(
                f"Circuit Breaker: размер позиции {signal.symbol} уменьшен "
                f"до {cb_mult*100:.0f}% ({self.guards.consecutive_losses} убытков подряд)"
            )

        # Проверка минимального лота ДО отправки ордера — иначе биржа/ccxt отклоняет
        # его как "amount must be greater than minimum amount precision" (наблюдалось
        # на COHR/IREN при уменьшенном Circuit Breaker'ом риске, см.
        # db-audit-august-2026), а такая ошибка ошибочно засчитывалась в error cascade
        # (_track_error) и уводила рабочий символ в кулдаун/бан на пустом месте.
        sl_distance_est = reference_price * (self.config.stop_loss_pct / 100)
        est_quantity = risk_budget / sl_distance_est if sl_distance_est > 0 else 0
        min_amount = await self._connector.min_order_amount(signal.symbol)
        if min_amount and est_quantity < min_amount:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: расчётный размер {est_quantity:.6f} "
                f"< минимального лота биржи {min_amount} при риске ${risk_budget:.2f} "
                f"— депозит слишком мал для этой монеты по текущей цене"
            )
            return (
                None,
                "amount_too_small",
                f"qty={est_quantity:.6f} min={min_amount} risk=${risk_budget:.2f}",
            )

        try:
            lev = int(self.config.leverage)
            if lev > 1:
                await self._connector.set_leverage(signal.symbol, lev)
        except Exception as e:
            logger.warning(f"Не удалось выставить плечо для {signal.symbol}: {e}")

        if self.config.pending_entry_pullback_pct > 0:
            return await self._place_pending_entry(
                session, signal, signal_id, reference_price, risk_budget,
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
        # Цена, по которой сетап был замечен, до отправки ордера. Ниже entry_price
        # перезаписывается фактической ценой заполнения с биржи, поэтому без этого
        # якоря реальное проскальзывание нигде не остаётся и его нечем измерить
        # (до 25.08.2026 trades.signal_price заполнялся только в pending-ветке, и у
        # всех алго-сделок был NULL — а бэктест при этом закладывал допущение
        # backtest_slippage_pct вслепую).
        reference_price = entry_price
        sl_distance = entry_price * (self.config.stop_loss_pct / 100)
        tp_distance = sl_distance * self.config.risk_reward_ratio
        quantity = risk_budget / sl_distance

        # Шаг лота биржи — последнее слово. ccxt всё равно обрежет объём до него
        # при отправке, поэтому обрезаем сами: иначе в БД попадёт объём, которого
        # на бирже нет. См. ExchangeConnector.amount_to_precision.
        quantity = await self._connector.amount_to_precision(signal.symbol, quantity)
        if quantity <= 0:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: расчётный размер меньше шага "
                f"лота биржи при риске ${risk_budget:.2f}"
            )
            return None, "amount_too_small", f"qty=0 после округления, risk=${risk_budget:.2f}"

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
            await self._connector.create_market_order(
                symbol=signal.symbol,
                side="buy",
                amount=quantity,
            )

            # 2. Ждём исполнения и получаем фактическую цену с биржи
            await asyncio.sleep(2)
            try:
                ex_positions = await self._connector.fetch_positions(
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
                    filled = abs(ex_positions[0].get("contracts") or 0)
                    if filled and filled != quantity:
                        logger.info(
                            f"Объём с биржи: расчёт={quantity} → факт={filled}"
                        )
                        quantity = filled
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
                await self._connector.set_tpsl(
                    symbol=signal.symbol,
                    side="buy",
                    amount=quantity,
                    tp_price=tp_price,
                    sl_price=sl_price,
                    tp_as_limit=self.config.tp_as_limit_order,
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
                        await self._connector.close_position(
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

            # 4. Частичная фиксация — лимитный ордер на partial_close_qty_pct% позиции
            # Выставляется сразу при открытии, не зависит от цикла опроса.
            if tp_sl_ok:
                try:
                    partial_trigger = self._partial_trigger_price(entry_price, tp_price)
                    partial_qty = await self._partial_qty(signal.symbol, quantity)
                    if partial_qty <= 0:
                        logger.info(
                            f"Частичная фиксация {signal.symbol} недоступна: "
                            f"{self.config.partial_close_qty_pct:.0f}% от {quantity} "
                            f"меньше шага лота биржи — на триггере стоп уйдёт в б/у "
                            f"без закрытия части"
                        )
                    elif await self._connector.place_reduce_only_limit(
                        symbol=signal.symbol,
                        side="buy",
                        amount=partial_qty,
                        price=partial_trigger,
                    ):
                        logger.info(
                            f"Лимитник частичной фиксации {signal.symbol}: "
                            f"{partial_qty:.2f} контрактов @ {partial_trigger:.6f} "
                            f"({self.config.partial_close_pct:.0f}% пути до TP)"
                        )
                    else:
                        logger.warning(
                            f"Лимитник частичной фиксации {signal.symbol} не принят "
                            f"биржей — будет проверка по циклу"
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
            if _is_agreement_error(err):
                self.guards.ban_symbol(signal.symbol)
                self.guards.track_error(signal.symbol)
                logger.info(
                    f"ByBit не даёт торговать {signal.symbol}: "
                    f"нужно подписать соглашение на сайте (добавлен в чёрный список)"
                )
                return None, "error", f"bybit_agreement: {err[:120]}"
            self.guards.track_error(signal.symbol)
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
            source=SOURCE,
            current_sl_price=sl_price if tp_sl_ok else None,
            signal_price=reference_price,
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
        self.guards.reset_errors(signal.symbol)
        return trade, "opened", None

    async def _place_pending_entry(
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None,
        reference_price: float,
        risk_budget: float,
    ) -> tuple[Trade | None, str, str | None]:
        """Вход лимитным ордером на откате от цены сигнала (решает проблему
        покупки на пике пампа). TP/SL и partial-close выставляются позже,
        при исполнении лимитника — см. `check_pending_entries()`."""
        pullback_pct = self.config.pending_entry_pullback_pct
        limit_price = reference_price * (1 - pullback_pct / 100)
        sl_distance = limit_price * (self.config.stop_loss_pct / 100)
        quantity = risk_budget / sl_distance
        quantity = await self._connector.amount_to_precision(signal.symbol, quantity)
        if quantity <= 0:
            logger.info(
                f"Сигнал {signal.symbol} пропущен: расчётный размер меньше шага "
                f"лота биржи при риске ${risk_budget:.2f}"
            )
            return None, "amount_too_small", f"qty=0 после округления, risk=${risk_budget:.2f}"

        logger.info(
            f"Pending-вход {signal.symbol}: сигнал=${reference_price:.6f} → "
            f"лимит=${limit_price:.6f} (откат {pullback_pct}%), "
            f"qty={quantity:.2f}, размер=${quantity * limit_price:.0f}"
        )

        try:
            await self._connector.create_limit_order(
                symbol=signal.symbol, side="buy", amount=quantity, price=limit_price,
            )
        except Exception as e:
            err = str(e)
            if _is_agreement_error(err):
                self.guards.ban_symbol(signal.symbol)
                self.guards.track_error(signal.symbol)
                logger.info(
                    f"ByBit не даёт торговать {signal.symbol}: "
                    f"нужно подписать соглашение на сайте (добавлен в чёрный список)"
                )
                return None, "error", f"bybit_agreement: {err[:120]}"
            self.guards.track_error(signal.symbol)
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
            source=SOURCE,
            signal_price=reference_price,
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
        self.guards.reset_errors(signal.symbol)
        return trade, "pending", None

    # ==================================================================
    # PENDING ENTRIES — лимитники на вход, ожидающие отката
    # ==================================================================

    async def check_pending_entries(self, session: AsyncSession) -> list[Trade]:
        """Проверить лимитники на вход: исполнились или истёк таймаут.
        Возвращает список позиций, активированных в этом вызове (для логирования)."""
        stmt = (
            select(Trade)
            .where(Trade.status == "pending", Trade.source == SOURCE)
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
                ex_positions = await self._connector.fetch_positions(pos.symbol)
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
        filled = abs(ex_position.get("contracts") or 0)
        if filled and filled != pos.quantity:
            logger.info(f"Объём с биржи: расчёт={pos.quantity} → факт={filled}")
            pos.quantity = filled

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
        self.guards.reset_errors(pos.symbol)

    async def _setup_tp_sl_and_partial(
        self, pos: Trade, fill_price: float, is_maker: bool
    ) -> tuple[float, float]:
        """Общая часть активации позиции после исполнения входа (лимитником или
        market-ордером): комиссия, TP/SL, лимитник частичной фиксации. Возвращает
        (tp_price, sl_price). Используется `_activate_pending_entry` — механическое
        исполнение лимитника входа."""
        pos.fee = (pos.fee or 0.0) + self._fee(pos.quantity * fill_price, taker=not is_maker)

        tp_price = self._tp_price(fill_price)
        sl_price = self._sl_price(fill_price)
        tp_sl_ok = False
        try:
            await self._connector.set_tpsl(
                symbol=pos.symbol,
                side="buy",
                amount=pos.quantity,
                tp_price=tp_price,
                sl_price=sl_price,
                tp_as_limit=self.config.tp_as_limit_order,
            )
            tp_sl_ok = True
        except Exception as e:
            logger.warning(f"TP/SL для {pos.symbol} будут выставлены в следующем цикле: {e}")
        pos.tp_sl_set = tp_sl_ok
        pos.current_sl_price = sl_price if tp_sl_ok else None

        if tp_sl_ok:
            try:
                partial_trigger = self._partial_trigger_price(fill_price, tp_price)
                partial_qty = await self._partial_qty(pos.symbol, pos.quantity)
                if partial_qty <= 0:
                    logger.info(
                        f"Частичная фиксация {pos.symbol} недоступна: доля позиции "
                        f"меньше шага лота биржи"
                    )
                elif not await self._connector.place_reduce_only_limit(
                    symbol=pos.symbol,
                    side="buy",
                    amount=partial_qty,
                    price=partial_trigger,
                ):
                    logger.warning(
                        f"Лимитник частичной фиксации {pos.symbol} не принят биржей"
                    )
            except Exception:
                logger.warning(
                    f"Не удалось выставить лимитник частичной фиксации для {pos.symbol}"
                )

        return tp_price, sl_price

    async def _expire_pending_entry(self, pos: Trade) -> None:
        """Лимитник на вход не исполнился за отведённое время — снять."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)
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
            .where(Trade.status == "open", Trade.source == SOURCE)
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
            ex_positions = await self._connector.fetch_positions()
            await self._clear_exchange_error()
            ex_symbols = {p["symbol"] for p in ex_positions}
        except Exception as e:
            logger.exception("Ошибка получения позиций с биржи")
            await self._alert_exchange_error("fetch_positions (обновление позиций)", e)
            return []

        for pos in db_positions:
            if not pos.tp_sl_set:
                if await self._resync_missing_tpsl(session, pos, closed):
                    continue

            current_price = await self._get_current_price(session, pos.symbol)

            if pos.symbol not in ex_symbols:
                await self._close_from_exchange(session, pos, current_price, closed)
                continue

            if not pos.partial_closed and await self._check_limit_partial_fill(pos):
                continue

            if not pos.partial_closed and current_price:
                if await self._check_partial_close_fallback(pos, current_price):
                    continue

            if await self._check_time_exit(session, pos, now, current_price, closed):
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
            await self._connector.set_tpsl(
                symbol=pos.symbol,
                side="buy",
                amount=pos.quantity,
                tp_price=tp,
                sl_price=sl,
                tp_as_limit=self.config.tp_as_limit_order,
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
                await self._connector.close_position(pos.symbol)
            except Exception:
                logger.exception(f"Не удалось аварийно закрыть {pos.symbol}")
            current_price = await self._get_current_price(session, pos.symbol)
            await self._close_position(pos, current_price or pos.entry_price, "sl", session)
            closed.append(pos)
            return True

    async def _close_from_exchange(
        self, session: AsyncSession, pos: Trade, current_price: float | None, closed: list[Trade]
    ) -> None:
        """Позиция уже закрыта на бирже (сработал TP/SL) — синхронизировать в БД."""
        exit_price = current_price or pos.entry_price
        # Пытаемся получить фактическую цену выхода. Если не вышло — закрываем по
        # последнему тикеру, но обязательно с логом: иначе PnL сделки молча
        # считался по приблизительной цене, и в отчётах это было неотличимо от
        # точного учёта (fee/PnL расходились с биржей без всякого следа).
        try:
            last_trade = await self._connector.fetch_last_trade(
                pos.symbol, pos.entry_time
            )
            if last_trade:
                exit_price = last_trade["price"]
            else:
                logger.warning(
                    f"{pos.symbol}: биржа не вернула сделку выхода — PnL посчитан "
                    f"по последнему тикеру (${exit_price:.6f}), возможна неточность"
                )
        except Exception as e:
            logger.warning(
                f"{pos.symbol}: не удалось получить фактическую цену выхода ({e}) — "
                f"PnL посчитан по последнему тикеру (${exit_price:.6f})"
            )
        await self._close_position(pos, exit_price, "tp_sl_exchange", session)
        closed.append(pos)

    def _partial_trigger_price(self, entry_price: float, tp_price: float) -> float:
        """Цена срабатывания частичной фиксации (% пути от входа до TP)."""
        return entry_price + (tp_price - entry_price) * (
            self.config.partial_close_pct / 100
        )

    async def _partial_qty(self, symbol: str, quantity: float) -> float:
        """Доля позиции под частичную фиксацию, округлённая до шага лота биржи.

        0.0 означает, что фиксировать нечем: шаг лота крупнее доли. Так бывает на
        малом депозите у монет с грубым шагом (SUI на bybit — 10 контрактов при
        позиции в 10), и раньше в этом случае уходил заведомо отбойный ордер.
        """
        return await self._connector.amount_to_precision(
            symbol, quantity * (self.config.partial_close_qty_pct / 100)
        )

    async def _check_limit_partial_fill(self, pos: Trade) -> bool:
        """Проверить исполнение лимитника частичной фиксации.

        Возвращает True, если лимитник исполнился и позиция обработана.
        """
        try:
            ex_positions = await self._connector.fetch_positions(
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
            await self._connector.set_tpsl(
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=actual_contracts,
                tp_price=self._tp_price(pos.entry_price),
                sl_price=pos.entry_price,
                tp_as_limit=self.config.tp_as_limit_order,
            )
            pos.current_sl_price = pos.entry_price
        except Exception as e:
            logger.warning(f"Не удалось перевести стоп в б/у для {pos.symbol}: {e}")

        pnl_pct = (trigger / pos.entry_price - 1) * 100
        await self._notify(
            f"🔒 <b>Частичная фиксация (лимитник)</b> {pos.direction.upper()}\n"
            f"Монета: {pos.symbol}\n"
            f"Закрыто {self.config.partial_close_qty_pct:.0f}% @ ${trigger:.6f}\n"
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

        close_qty = await self._partial_qty(pos.symbol, pos.quantity)
        if close_qty <= 0:
            # Закрыть часть нечем — шаг лота крупнее доли. Но вторая половина
            # механизма (перевод стопа в безубыток) от объёма не зависит, а
            # защищает она больше, чем сама фиксация. Идемпотентно по
            # current_sl_price, иначе set_tpsl уходил бы каждый цикл.
            if pos.current_sl_price != pos.entry_price:
                try:
                    await self._connector.set_tpsl(
                        symbol=pos.symbol,
                        side="buy" if pos.direction == "long" else "sell",
                        amount=pos.quantity,
                        tp_price=self._tp_price(pos.entry_price),
                        sl_price=pos.entry_price,
                        tp_as_limit=self.config.tp_as_limit_order,
                    )
                    pos.current_sl_price = pos.entry_price
                    logger.info(
                        f"Частичная фиксация {pos.symbol} невозможна (шаг лота), "
                        f"стоп переведён в безубыток"
                    )
                except Exception as e:
                    logger.warning(f"Перевод стопа в б/у для {pos.symbol}: {e}")
            return True

        # Проверить, нет ли уже лимитника на бирже (после рестарта).
        # Сбой этого запроса НЕ означает "ордеров нет": раньше исключение здесь
        # молча проглатывалось, флаг оставался False, и код отправлял market
        # reduce-only ПОВЕРХ живого лимитника частичной фиксации — закрывалось
        # вдвое больше запланированного, без единой строчки в логе. Любая
        # неопределённость = пропустить цикл, повторим на следующем.
        try:
            open_orders = await self._connector.fetch_open_orders(
                pos.symbol
            )
        except Exception as e:
            logger.warning(
                f"Частичная фиксация {pos.symbol}: не удалось проверить открытые "
                f"ордера ({e}) — fallback пропущен, повтор в следующем цикле"
            )
            return True

        if open_orders:
            logger.info(
                f"Частичная фиксация {pos.symbol}: "
                f"на бирже есть открытые ордера, "
                f"пропускаем fallback (лимитник уже работает)"
            )
            return True

        try:
            await self._connector.create_market_reduce_order(
                pos.symbol,
                "sell" if pos.direction == "long" else "buy",
                close_qty,
            )
            # Получаем фактический остаток и переводим стоп в б/у
            ex_positions = await self._connector.fetch_positions(
                pos.symbol
            )
            remaining = pos.quantity - close_qty
            if ex_positions:
                remaining = abs(ex_positions[0]["contracts"])
            await self._connector.set_tpsl(
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=remaining,
                tp_price=self._tp_price(pos.entry_price),
                sl_price=pos.entry_price,
                tp_as_limit=self.config.tp_as_limit_order,
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
            f"Закрыто {self.config.partial_close_qty_pct:.0f}% @ ${current_price:.6f}\n"
            f"Частичный PnL: ${partial_pnl:+.2f} ({pnl_pct:+.1f}%)\n"
            f"Стоп переведён в безубыток"
        )
        logger.info(
            f"Частичное закрытие: {pos.symbol} {self.config.partial_close_qty_pct:.0f}% @ {current_price:.6f}"
        )
        return True

    async def _check_time_exit(
        self,
        session: AsyncSession,
        pos: Trade,
        now: datetime,
        current_price: float | None,
        closed: list[Trade],
    ) -> bool:
        """Закрыть позицию по истечении max_hold_hours.

        Возвращает True, если стадия обработана (позиция закрыта, либо
        закрытие не удалось и будет повторено в следующем цикле).
        """
        deadline = pos.entry_time.replace(tzinfo=timezone.utc) + timedelta(
            hours=self.config.max_hold_hours
        )
        if now < deadline:
            return False

        try:
            await self._connector.close_position(pos.symbol)
        except Exception:
            logger.exception(f"Ошибка закрытия {pos.symbol} по времени")
            return True

        exit_price = current_price or pos.entry_price
        await self._close_position(pos, exit_price, "time", session)
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
        """Открытые позиции + pending-заявки на вход (обе занимают "слот" max_positions)."""
        from sqlalchemy import func
        stmt = (
            select(func.count()).select_from(Trade)
            .where(Trade.status.in_(["open", "pending"]), Trade.source == SOURCE)
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
                Trade.source == SOURCE,
            )
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.first() is not None

    async def _has_position(self, session: AsyncSession, symbol: str) -> bool:
        """Есть ли уже открытая позиция или pending-заявка на вход по символу в своём пайплайне."""
        stmt = (
            select(Trade)
            .where(Trade.symbol == symbol, Trade.status.in_(["open", "pending"]), Trade.source == SOURCE)
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.first() is not None

    async def _get_current_price(
        self, session: AsyncSession, symbol: str
    ) -> float | None:
        """Последняя известная цена — ОБЯЗАТЕЛЬНО с биржи, на которой реально
        торгует этот пайплайн (self._connector), а не любая свежайшая запись
        по символу. Ticker собирается с нескольких бирж (данные — не только
        Bybit, см. vibetrade-signal-detector-cross-exchange), и по 532 символам
        в проде есть записи и от Binance, и от Bybit одновременно — без
        фильтра по exchange здесь можно было получить цену с ЧУЖОЙ биржи
        (например, Binance) и использовать её как reference_price для ордера,
        который реально уходит в стакан Bybit (лимитник на откате, fallback
        частичного закрытия, выход по времени).

        Это СОХРАНЁННАЯ цена: сборщик обновляет тикер раз в цикл, то есть значение
        может отставать примерно на длину скана. Для мониторинга это нормально —
        он и сам идёт раз в цикл. Для входа в позицию нет, см. `_get_entry_price`."""
        stmt = (
            select(Ticker.last)
            .where(Ticker.symbol == symbol, Ticker.exchange == self._connector.exchange_id)
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.first()
        return row[0] if row else None

    async def _get_entry_price(self, session: AsyncSession, symbol: str) -> float | None:
        """Цена для расчёта входа — живым запросом к бирже, с фолбэком на БД.

        Раньше вход считался по сохранённому тикеру, а тот обновляется раз в цикл
        сбора: на проде интервал между записями по одной монете был около 135 секунд.
        Market-ордер при этом исполняется по ЖИВОЙ цене, и вся разница оседала в
        расхождении между `signal_price` и фактической ценой заполнения — то есть
        ровно в том проскальзывании, которое `backtest_slippage_pct` подбирал вслепую
        (см. комментарий к нему в config.yaml). Для стратегии, которая входит на
        быстром движении, две минуты отставания — это не мелочь.

        Фолбэк на сохранённую цену оставлен намеренно: не получить цену вообще хуже,
        чем получить слегка устаревшую, но об этом должно быть видно в логе."""
        try:
            ticker = await self._connector.fetch_ticker(symbol)
            price = ticker.get("last")
            if price:
                return float(price)
            logger.warning(f"{symbol}: биржа вернула тикер без цены, беру сохранённую")
        except Exception as e:
            logger.warning(
                f"{symbol}: не удалось получить живую цену ({e}) — вход считается "
                f"по сохранённому тикеру, возможен увеличенный слиппедж"
            )
        return await self._get_current_price(session, symbol)

    async def _close_position(
        self, trade: Trade, exit_price: float, reason: str,
        session: AsyncSession | None = None,
    ) -> None:
        """`session` (если передан вызывающим) прокидывается в `guards.save()` —
        см. её docstring про фикс гонки за блокировкой БД 12.08.2026."""
        trade.exit_price = exit_price
        trade.exit_time = datetime.now(tz=timezone.utc)
        trade.status = "closed"

        # PnL оставшейся части
        if trade.direction == "long":
            remainder_pnl = (exit_price - trade.entry_price) * trade.quantity
        else:
            remainder_pnl = (trade.entry_price - exit_price) * trade.quantity

        prior_fee = trade.fee or 0.0
        exit_notional = exit_price * trade.quantity

        # Предварительно считаем комиссию как taker (верно для SL/time/
        # аварийного закрытия — они всегда market) — этого достаточно, чтобы
        # определить знак PnL и, для tp_sl_exchange, отличить TP от SL ниже.
        trade.fee = prior_fee + self._fee(exit_notional, taker=True)
        total_pnl = remainder_pnl + (trade.partial_pnl or 0.0) - trade.fee
        trade.pnl = total_pnl

        # Если закрыто биржей — определяем TP или SL по PnL (sync-цикл видит
        # только «позиция исчезла», не какой именно ордер её закрыл)
        if reason == "tp_sl_exchange":
            reason = "tp" if (trade.pnl or 0) > 0 else "sl"

        # TP закрывается лимитным ордером (maker), если включено конфигом
        # (tp_as_limit_order) — пересчитываем комиссию/PnL по реальной ставке.
        if reason == "tp" and self.config.tp_as_limit_order:
            trade.fee = prior_fee + self._fee(exit_notional, taker=False)
            total_pnl = remainder_pnl + (trade.partial_pnl or 0.0) - trade.fee
            trade.pnl = total_pnl

        pnl_pct = (
            (exit_price / trade.entry_price - 1) * 100
            if trade.direction == "long"
            else (trade.entry_price / exit_price - 1) * 100
        )

        # Circuit Breaker: обновляем счётчик убытков подряд
        if self.config.circuit_breaker_enabled:
            if (trade.pnl or 0) <= 0:
                self.guards.register_loss()
                logger.warning(
                    f"Circuit Breaker: {self.guards.consecutive_losses} убытков подряд "
                    f"(PnL=${trade.pnl:+.2f} на {trade.symbol})"
                )
                if self.guards.consecutive_losses >= self.config.circuit_breaker_loss_streak_reduce:
                    mult = self.config.circuit_breaker_reduce_mult_pct
                    logger.warning(
                        f"Circuit Breaker: размер позиций уменьшен до {mult:.0f}%"
                    )
            else:
                if self.guards.consecutive_losses > 0:
                    logger.info(
                        f"Circuit Breaker: серия из {self.guards.consecutive_losses} убытков "
                        f"прервана прибылью ${trade.pnl:+.2f} на {trade.symbol}"
                    )
                self.guards.register_win()
            await self.guards.save(session)

        labels = {
            "tp": ("✅", "Тейк-профит"),
            "sl": ("🛑", "Стоп-лосс"),
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
        """Положить уведомление в очередь, а не слать немедленно.

        Отправка идёт по сети, а вызывается отсюда изнутри транзакции цикла
        сбора — то есть write-лок SQLite удерживался на время HTTP-запроса к
        Telegram. Теперь сообщения копятся и уходят из `flush_notifications()`
        уже ПОСЛЕ коммита (см. `Application._on_collect_cycle_done`).

        Цена решения: если процесс упадёт между действием и flush, уведомление
        потеряется. Это осознанно — источник правды по сделкам всё равно БД,
        а держать лок ради телеметрии дороже."""
        if self._send_message:
            self._outbox.append(text)

    async def flush_notifications(self) -> None:
        """Отправить накопленные уведомления. Вызывать ПОСЛЕ commit()."""
        if not self._send_message or not self._outbox:
            return
        pending, self._outbox = self._outbox, []
        for text in pending:
            try:
                await self._send_message(text)
            except Exception:
                logger.exception("Ошибка отправки торгового уведомления")

    # ------------------------------------------------------------------
    # Алерт на ошибки связи с биржей (истёкший API-ключ и т.п.)
    # ------------------------------------------------------------------

    _EXCHANGE_ERROR_ALERT_COOLDOWN = timedelta(minutes=30)
    _EXCHANGE_ERROR_AUTH_HINTS = (
        "expired", "api_key", "apikey", "invalid", "signature", "unauthorized",
        "401", "403", "10003", "10004", "33004",  # характерные коды Bybit
    )

    async def _alert_exchange_error(self, context: str, error: Exception) -> None:
        """Уведомить в Telegram об ошибке связи с биржей (например, протухший
        API-ключ, см. db-audit-august-19-2026: ~40ч простоя без алертинга).

        Не спамит на каждую попытку: первый алерт — сразу, повторные — не чаще
        раза в _EXCHANGE_ERROR_ALERT_COOLDOWN, пока ошибка не исчезнет
        (см. _clear_exchange_error)."""
        now = datetime.now(tz=timezone.utc)
        first_occurrence = self._exchange_error_since is None
        if first_occurrence:
            self._exchange_error_since = now

        if not first_occurrence and self._exchange_error_last_alert_at is not None:
            if now - self._exchange_error_last_alert_at < self._EXCHANGE_ERROR_ALERT_COOLDOWN:
                return

        self._exchange_error_last_alert_at = now
        down_for = ""
        if not first_occurrence:
            minutes = int((now - self._exchange_error_since).total_seconds() // 60)  # type: ignore[operator]
            down_for = f" (продолжается {minutes} мин)"

        err_str = str(error).lower()
        hint = ""
        if any(marker in err_str for marker in self._EXCHANGE_ERROR_AUTH_HINTS):
            hint = "\n⚠️ Похоже на протухший/неверный API-ключ — проверь ключ на бирже."

        logger.error(f"Ошибка связи с биржей ({context}): {error}")
        await self._notify(
            f"🔴 <b>Ошибка связи с биржей</b>{down_for}\n"
            f"Контекст: {context}\n"
            f"{error}"
            f"{hint}"
        )

    async def _clear_exchange_error(self) -> None:
        """Вызывать после успешного запроса к бирже — сбрасывает состояние
        алерта и, если до этого был алерт о простое, шлёт уведомление о восстановлении."""
        if self._exchange_error_since is None:
            return
        now = datetime.now(tz=timezone.utc)
        minutes = int((now - self._exchange_error_since).total_seconds() // 60)
        self._exchange_error_since = None
        self._exchange_error_last_alert_at = None
        await self._notify(
            f"🟢 Связь с биржей восстановлена (простой ~{minutes} мин)"
        )

    # ------------------------------------------------------------------
    # Error cascade protection
    # ------------------------------------------------------------------


