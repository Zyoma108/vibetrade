"""ИИ-АГЕНТ — исполнительные примитивы (source='agent' только).

Решения принимает оркестратор (Claude Code /loop + сабагенты entry-agent/reeval-agent),
эти методы вызываются из scripts/agent_actions.py — не из Python-цикла бота. Здесь же
жёсткие рельсы (SL нельзя ослабить, TP нельзя понизить, продление капается конфигом,
откат клэмпится диапазоном), которые действуют независимо от того, что попросила модель.

Вынесено в отдельный класс-наследник `PositionManager`, а не в сам PositionManager,
чтобы менять/расширять поведение ИИ-режима, не трогая файл алго-режима и не рискуя
его поведением — см. AGENTS.md, "ИИ-режим"."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.base import Signal
from src.executor.position_manager import PositionManager
from src.storage.models import Trade

logger = logging.getLogger(__name__)


class AgentPositionManager(PositionManager):
    """Всё, что решает ИИ-агент по своей сделке: способ входа, сопровождение
    открытой позиции, управление ещё неисполненным лимитником. Общая механика
    (guard-проверки входа, TP/SL/partial-close при исполнении, мониторинг
    pending/open) — в базовом `PositionManager`, здесь только LLM-решения."""

    # ------------------------------------------------------------------
    # Гонка LLM-решения с механическим циклом (_agent_position_loop опрашивает
    # биржу независимо, пока идёт LLM round-trip) — перед любым действием
    # перепроверяем фактическое состояние НА БИРЖЕ, не только Trade.status в БД
    # (он может отставать на длину цикла). См. AGENTS.md, план "ИИ-агент: гибкость
    # входа и сопровождения позиции".
    # ------------------------------------------------------------------

    async def _exchange_has_open_position(self, symbol: str) -> bool:
        try:
            positions = await self._connector.fetch_positions(symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent: не удалось проверить состояние {symbol} на бирже")
            return False
        return bool(positions)

    # ------------------------------------------------------------------
    # Вход — entry-agent выбирает market или лимитник со своим откатом
    # ------------------------------------------------------------------

    async def open_position(  # type: ignore[override]
        self,
        session: AsyncSession,
        signal: Signal,
        signal_id: int | None = None,
        entry_mode: str = "limit",
        pullback_pct: float | None = None,
    ) -> tuple[Trade | None, str, str | None]:
        """Тонкая обёртка над `PositionManager.open_position`: вся защитная логика
        (режим/Circuit Breaker/лимиты/кулдауны/баланс/risk_budget) остаётся в
        базовом классе, здесь — только перевод решения entry-agent в его параметры."""
        return await super().open_position(
            session,
            signal,
            signal_id=signal_id,
            force_market=(entry_mode == "market"),
            pullback_pct_override=pullback_pct,
        )

    # ------------------------------------------------------------------
    # Открытая позиция — сопровождение
    # ------------------------------------------------------------------

    def _current_sl_price(self, pos: Trade) -> float:
        """Последний известный эффективный стоп. Нужен, чтобы гарантировать
        монотонность: агент может только подтягивать SL, никогда не ослаблять."""
        if pos.current_sl_price is not None:
            return pos.current_sl_price
        if pos.partial_closed:
            return pos.entry_price  # переведено в безубыток, столбец ещё не проставлен (старые строки)
        return self._sl_price(pos.entry_price)

    def _current_tp_price(self, pos: Trade) -> float:
        """Последний известный эффективный тейк. Симметрично `_current_sl_price` —
        агент может только поднимать TP, никогда не понижать."""
        if pos.current_tp_price is not None:
            return pos.current_tp_price
        return self._tp_price(pos.entry_price)

    async def apply_agent_tighten_sl(self, pos: Trade, new_sl_price: float) -> bool:
        """Подтянуть стоп по решению агента. Отклоняет попытку ослабить SL —
        это жёсткий рельс в коде, а не только инструкция модели."""
        current_sl = self._current_sl_price(pos)
        if new_sl_price <= current_sl:
            logger.warning(
                f"Agent: отклонена попытка ослабить SL для {pos.symbol} "
                f"({new_sl_price:.6f} <= текущий {current_sl:.6f})"
            )
            return False
        try:
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=pos.quantity,
                tp_price=self._current_tp_price(pos),
                sl_price=new_sl_price,
            )
        except Exception as e:
            logger.warning(f"Agent: не удалось подтянуть SL для {pos.symbol}: {e}")
            return False
        pos.current_sl_price = new_sl_price
        return True

    async def apply_agent_raise_tp(self, pos: Trade, new_tp_price: float) -> bool:
        """Поднять тейк по решению агента. Отклоняет попытку понизить TP —
        жёсткий рельс в коде, симметрично `apply_agent_tighten_sl`."""
        current_tp = self._current_tp_price(pos)
        if new_tp_price <= current_tp:
            logger.warning(
                f"Agent: отклонена попытка понизить TP для {pos.symbol} "
                f"({new_tp_price:.6f} <= текущий {current_tp:.6f})"
            )
            return False
        try:
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=pos.quantity,
                tp_price=new_tp_price,
                sl_price=self._current_sl_price(pos),
            )
        except Exception as e:
            logger.warning(f"Agent: не удалось поднять TP для {pos.symbol}: {e}")
            return False
        pos.current_tp_price = new_tp_price
        return True

    async def apply_agent_hold_extension(self, pos: Trade, extend_hours: float) -> bool:
        """Продлить дедлайн max_hold_hours. Капается и за раз, и суммарно
        конфигом agent.max_hold_extension_hours / max_hold_extension_total_hours."""
        cfg = self._agent_config
        if not cfg:
            return False
        extend_hours = max(0.0, min(extend_hours, cfg.max_hold_extension_hours))
        total_used = pos.llm_hold_extension_total_hours or 0.0
        remaining_budget = cfg.max_hold_extension_total_hours - total_used
        if remaining_budget <= 0:
            logger.info(f"Agent: лимит продлений удержания исчерпан для {pos.symbol}")
            return False
        extend_hours = min(extend_hours, remaining_budget)
        if extend_hours <= 0:
            return False

        base_deadline = pos.entry_time.replace(tzinfo=timezone.utc) + timedelta(
            hours=self.config.max_hold_hours
        )
        current_deadline = (
            pos.llm_hold_until.replace(tzinfo=timezone.utc) if pos.llm_hold_until else base_deadline
        )
        pos.llm_hold_until = current_deadline + timedelta(hours=extend_hours)
        pos.llm_hold_extension_total_hours = total_used + extend_hours
        return True

    async def apply_agent_close(
        self, session: AsyncSession, pos: Trade, current_price: float | None
    ) -> bool:
        """Закрыть позицию досрочно по решению агента (снимает висящий
        лимитник частичной фиксации перед закрытием)."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent close: не удалось отменить ордера для {pos.symbol}")
        try:
            await self._connector.close_position(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.exception(f"Agent close: не удалось закрыть {pos.symbol}")
            return False
        exit_price = current_price or pos.entry_price
        await self._close_position(pos, exit_price, "llm_close")
        return True

    async def apply_agent_partial_close(self, pos: Trade, current_price: float) -> bool:
        """Зафиксировать половину позиции по рынку немедленно, не дожидаясь
        автоматического триггера (`partial_close_pct`). Тот же процент (50%),
        что и у автоматической фиксации — не усложняем выбором произвольной
        доли. Не двигает SL — это отдельное самостоятельное решение
        (`apply_agent_tighten_sl`), агент управляет ими независимо."""
        if pos.partial_closed:
            logger.info(f"Agent: частичная фиксация {pos.symbol} уже была, повторно не поддерживается")
            return False

        close_qty = pos.quantity / 2
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent partial close: не удалось отменить ордера для {pos.symbol}")
        try:
            # Reduce-only market на половину позиции — тот же паттерн, что
            # уже используется в PositionManager._check_partial_close_fallback.
            await self._connector._call(  # type: ignore[union-attr]
                "create_order",
                pos.symbol, "market",
                "sell" if pos.direction == "long" else "buy",
                close_qty, None,
                {"reduceOnly": True},
            )
        except Exception as e:
            logger.exception(f"Agent partial close: не удалось закрыть часть {pos.symbol}: {e}")
            return False

        remaining = pos.quantity - close_qty
        try:
            ex_positions = await self._connector.fetch_positions(pos.symbol)  # type: ignore[union-attr]
            if ex_positions:
                remaining = abs(ex_positions[0]["contracts"])
            await self._connector.set_tpsl(  # type: ignore[union-attr]
                symbol=pos.symbol,
                side="buy" if pos.direction == "long" else "sell",
                amount=remaining,
                tp_price=self._current_tp_price(pos),
                sl_price=self._current_sl_price(pos),
            )
        except Exception as e:
            logger.warning(f"Agent partial close: не удалось переустановить TP/SL для {pos.symbol}: {e}")

        partial_pnl = (
            (current_price - pos.entry_price) * close_qty
            if pos.direction == "long"
            else (pos.entry_price - current_price) * close_qty
        )
        pos.quantity = remaining
        pos.partial_closed = True
        pos.partial_pnl = (pos.partial_pnl or 0.0) + partial_pnl
        pos.fee = (pos.fee or 0.0) + self._fee(current_price * close_qty, taker=True)
        logger.info(f"Agent: частичная фиксация {pos.symbol} {close_qty:.2f} @ {current_price:.6f}")
        return True

    # ------------------------------------------------------------------
    # Pending-заявка на вход — reeval-agent может подвинуть, перевести в
    # market или отменить, пока лимитник не исполнился
    # ------------------------------------------------------------------

    async def apply_agent_reprice_pending(
        self, session: AsyncSession, pos: Trade, new_pullback_pct: float
    ) -> bool:
        """Передвинуть неисполненный лимитник входа на новый откат
        (свежая референсная цена, тот же объём, полный новый таймаут)."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent reprice: не удалось отменить лимитник для {pos.symbol}")

        reference_price = await self._get_current_price(session, pos.symbol)
        if reference_price is None or reference_price <= 0:
            logger.warning(f"Agent reprice: нет цены для {pos.symbol}")
            return False
        new_limit_price = reference_price * (1 - new_pullback_pct / 100)
        try:
            await self._connector.create_limit_order(  # type: ignore[union-attr]
                symbol=pos.symbol, side="buy", amount=pos.quantity, price=new_limit_price,
            )
        except Exception as e:
            logger.warning(f"Agent reprice: не удалось выставить новый лимитник для {pos.symbol}: {e}")
            return False

        pos.entry_price = new_limit_price
        pos.pending_expires_at = datetime.now(tz=timezone.utc) + timedelta(
            minutes=self.config.pending_entry_timeout_minutes
        )
        logger.info(f"Agent: лимитник {pos.symbol} передвинут на ${new_limit_price:.6f} (откат {new_pullback_pct}%)")
        return True

    async def apply_agent_convert_to_market(self, pos: Trade) -> bool:
        """Снять неисполненный лимитник и войти по рынку немедленно —
        тем же объёмом, дальше стандартная настройка TP/SL и лимитника
        частичной фиксации (общий `_setup_tp_sl_and_partial`)."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent convert-to-market: не удалось отменить лимитник для {pos.symbol}")
        try:
            raw = await self._connector.create_market_order(  # type: ignore[union-attr]
                symbol=pos.symbol, side="buy", amount=pos.quantity,
            )
        except Exception as e:
            logger.exception(f"Agent convert-to-market: не удалось войти по рынку для {pos.symbol}: {e}")
            return False

        fill_price = raw.get("fill_price") or pos.entry_price
        pos.entry_price = fill_price
        pos.entry_time = datetime.now(tz=timezone.utc)
        pos.status = "open"
        await self._setup_tp_sl_and_partial(pos, fill_price, is_maker=False)
        logger.info(f"Agent: pending переведён в market {pos.symbol} @ {fill_price:.6f}")
        return True

    async def apply_agent_cancel_pending(self, pos: Trade) -> bool:
        """Отменить неисполненный лимитник — сетап больше не актуален
        (в отличие от `expired` — это решение агента, не таймаут)."""
        try:
            await self._connector.cancel_all_orders(pos.symbol)  # type: ignore[union-attr]
        except Exception:
            logger.warning(f"Agent cancel-pending: не удалось отменить лимитник для {pos.symbol}")
        pos.status = "cancelled"
        logger.info(f"Agent: лимитник {pos.symbol} отменён по решению агента")
        return True
