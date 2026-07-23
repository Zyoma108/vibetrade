"""Инструменты (tools) для ИИ-агента — только чтение данных, без побочных
эффектов на торговлю. Решения о входе/сопровождении принимает оркестратор
(Claude Code /loop-скилл + сабагенты entry-agent/reeval-agent), исполнение —
PositionManager (отдельный аккаунт, source='agent') через scripts/agent_actions.py.

Каждый инструмент отдаёт агрегированные компактные метрики, а не сырые дампы
API — иначе контекст LLM быстро раздувается шумом (см. AGENTS.md, раздел
про ИИ-режим). Диспетчер (`AgentToolkit.dispatch`) вызывается из
scripts/agent_data.py — это единственный Bash-инструмент, разрешённый
сабагентам."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.data_provider import CandleCache
from src.config import StrategyConfig, TradingConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import (
    AgentDecision,
    Candle,
    MarketContextSnapshot,
    OpenInterest,
    Signal,
    Ticker,
    Trade,
)

logger = logging.getLogger(__name__)

# Версия инструкций сабагентов/оркестратора — логируется с каждым решением
# (AgentDecision.agent_version), чтобы позже сопоставить качество решений с
# конкретной редакцией .claude/agents/*.md и .claude/skills/vibetrade-agent-loop.
# Бампнуть при значимой правке промптов/скилла.
AGENT_VERSION = "v3-flexible-execution"


class AgentToolkit:
    """Один экземпляр на вызов сабагента (короткоживущий процесс
    scripts/agent_data.py). Копит трейс вызовов в self.calls, если вызывающий
    код хочет его залогировать (не обязательно — оркестратор сам решает, что
    писать в agent_decisions)."""

    def __init__(
        self,
        session: AsyncSession,
        connector: ExchangeConnector,
        candle_cache: CandleCache | None = None,
        trading_config: TradingConfig | None = None,
    ):
        self._session = session
        self._connector = connector
        self._candle_cache = candle_cache
        self._trading_config = trading_config
        self.calls: list[dict] = []

    async def dispatch(self, name: str, tool_input: dict) -> dict:
        handler = getattr(self, f"_tool_{name}", None)
        if handler is None:
            result = {"error": f"unknown tool {name}"}
        else:
            try:
                result = await handler(**tool_input)
            except Exception as e:
                logger.exception(f"Agent tool {name} failed")
                result = {"error": str(e)}
        self.calls.append({"tool": name, "input": tool_input, "output": result})
        return result

    async def _current_price(self, symbol: str) -> float | None:
        stmt = (
            select(Ticker.last)
            .where(Ticker.symbol == symbol)
            .order_by(desc(Ticker.timestamp))
            .limit(1)
        )
        row = (await self._session.execute(stmt)).first()
        return row[0] if row else None

    async def _resolve_exchange(self, model: type[Candle] | type[OpenInterest], symbol: str) -> str:
        """Сборщик (src/collectors/market_data.py) не дублирует свечи/OI для монет,
        торгуемых и на Bybit, и на Binance одновременно — они пишутся только под
        exchange='binance'. Если на бирже агентского аккаунта для символа пусто,
        падаем на binance вместо того, чтобы считать данные отсутствующими."""
        primary = self._connector.exchange_id
        stmt = select(model.id).where(model.exchange == primary, model.symbol == symbol).limit(1)
        if (await self._session.execute(stmt)).first():
            return primary
        stmt = select(model.id).where(model.exchange == "binance", model.symbol == symbol).limit(1)
        if (await self._session.execute(stmt)).first():
            return "binance"
        return primary

    # ------------------------------------------------------------------
    # Data tools
    # ------------------------------------------------------------------

    async def _tool_get_symbol_snapshot(self, symbol: str, bars: int = 30) -> dict:
        """Последние N свечей текущего таймфрейма: диапазон цены, объём, % изменения."""
        exchange = await self._resolve_exchange(Candle, symbol)
        if self._candle_cache:
            candles = await self._candle_cache.load_or_refresh(self._session, exchange, symbol, bars)
        else:
            stmt = (
                select(Candle)
                .where(Candle.exchange == exchange, Candle.symbol == symbol)
                .order_by(desc(Candle.timestamp)).limit(bars)
            )
            rows = (await self._session.execute(stmt)).scalars().all()
            candles = [
                {"open": r.open, "high": r.high, "low": r.low, "close": r.close, "volume": r.volume}
                for r in reversed(rows)
            ]
        if not candles:
            return {"error": "no candle data"}
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]
        return {
            "bars": len(candles),
            "first_close": closes[0],
            "last_close": closes[-1],
            "change_pct": round((closes[-1] / closes[0] - 1) * 100, 2) if closes[0] else None,
            "high": max(c["high"] for c in candles),
            "low": min(c["low"] for c in candles),
            "avg_volume": round(sum(volumes) / len(volumes), 2) if volumes else None,
            "last_volume": volumes[-1] if volumes else None,
        }

    async def _tool_get_oi_trend(self, symbol: str, n_bars: int = 10) -> dict:
        """Тренд открытого интереса за последние N точек. Рост OI вместе с
        объёмом = новые деньги; плоский/падающий OI при растущем объёме =
        вероятно перекладывание позиций, не новый спрос."""
        exchange = await self._resolve_exchange(OpenInterest, symbol)
        stmt = (
            select(OpenInterest.value)
            .where(OpenInterest.exchange == exchange, OpenInterest.symbol == symbol)
            .order_by(desc(OpenInterest.timestamp)).limit(n_bars)
        )
        values = list(reversed((await self._session.execute(stmt)).scalars().all()))
        if len(values) < 2:
            return {"error": "insufficient OI history"}
        slope_pct = (values[-1] / values[0] - 1) * 100 if values[0] else None
        return {
            "symbol": symbol,
            "points": len(values),
            "first_oi": values[0],
            "last_oi": values[-1],
            "slope_pct": round(slope_pct, 2) if slope_pct is not None else None,
        }

    async def _tool_get_funding_rate(self, symbol: str) -> dict:
        """Funding rate по перпетуалу. Сильно положительная = лонги уже
        перегружены, повышенный риск лонг-сквиза против нашей позиции."""
        result = await self._connector.fetch_funding_rate(symbol)
        return result or {"error": "funding rate unavailable"}

    async def _tool_get_order_book_summary(self, symbol: str) -> dict:
        """Спред и глубина стакана в USD на ±0.5%/±1% от средней цены —
        показывает исполнимость (можно ли войти/выйти без сильного проскальзывания)."""
        result = await self._connector.fetch_order_book_summary(symbol)
        return result or {"error": "order book unavailable"}

    async def _tool_get_symbol_pump_history(self, symbol: str, limit: int = 5) -> dict:
        """Последние N сигналов по монете и их исход — история повторных пампов
        (монета-рецидивист с историей разворотов — повод для осторожности)."""
        stmt = (
            select(Signal, Trade)
            .outerjoin(Trade, Trade.signal_id == Signal.id)
            .where(Signal.symbol == symbol)
            .order_by(desc(Signal.timestamp))
            .limit(limit)
        )
        rows = (await self._session.execute(stmt)).all()
        history = []
        for sig, trade in rows:
            entry: dict = {"timestamp": sig.timestamp.isoformat(), "confidence": sig.confidence}
            if trade:
                entry.update({"traded": True, "status": trade.status, "pnl": trade.pnl})
            else:
                entry.update({"traded": False, "missed_reason": sig.missed_reason})
            history.append(entry)
        return {"symbol": symbol, "past_signals": history}

    async def _tool_get_market_context(self) -> dict:
        """Текущий рыночный режим (risk_on/cautious/risk_off), тренд и изменение
        цены BTC/OTHERS за 1ч и 4ч. Читает последний снимок из БД (пишется ботом
        каждый цикл) — не требует живого подключения к TradingView/бирже."""
        stmt = select(MarketContextSnapshot).order_by(desc(MarketContextSnapshot.timestamp)).limit(1)
        snap = (await self._session.execute(stmt)).scalar_one_or_none()
        if not snap or not snap.ready:
            return {"error": "market context unavailable"}
        # Та же логика, что MarketContext.should_block_entries() — алго-режим в этих
        # случаях сам не открывает позицию. Агент решает независимо, но при
        # entries_restricted=true обязан требовать более сильных оснований (см.
        # entry-agent.md, "Рыночный режим ограничивает вход").
        entries_restricted = snap.regime == "risk_off" or (
            snap.regime == "cautious" and snap.supertrend_color == "red"
        )
        restriction_reason = None
        if snap.regime == "risk_off":
            restriction_reason = "risk_off — алгоритм не открывает позиции ни по одному сигналу"
        elif entries_restricted:
            restriction_reason = "cautious + Supertrend=red — по аудиту июня 2026 все сделки в этом сочетании были убыточны"
        return {
            "regime": snap.regime,
            "trend": snap.trend,
            "supertrend_color": snap.supertrend_color,
            "btc_change_1h": snap.btc_change_1h,
            "btc_change_4h": snap.btc_change_4h,
            "others_change_1h": snap.others_change_1h,
            "others_change_4h": snap.others_change_4h,
            "snapshot_age_minutes": round(
                (datetime.now(tz=timezone.utc) - snap.timestamp.replace(tzinfo=timezone.utc)).total_seconds() / 60, 1
            ),
            "entries_restricted": entries_restricted,
            "restriction_reason": restriction_reason,
        }

    async def _tool_get_higher_timeframe_history(
        self, symbol: str, timeframe: str = "4h", limit: int = 90
    ) -> dict:
        """OHLC-свечи на старшем таймфрейме (напр. 4h/1d) — для поиска уровней
        поддержки/сопротивления. Запрашивается напрямую с биржи по требованию,
        не хранится в БД."""
        candles = await self._connector.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not candles:
            return {"error": "no data"}
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "bars": len(candles),
            "candles": [
                {"t": c["timestamp"].isoformat(), "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"]}
                for c in candles
            ],
        }

    async def _tool_get_recent_signal_activity(self, minutes: int = 15) -> dict:
        """Сколько разных монет дали сигнал за последние N минут — дешёвый
        прокси для секторальной ротации (несколько монет пампят одновременно)
        против изолированного пампа одной монеты."""
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)
        stmt = select(Signal.symbol).where(Signal.timestamp >= cutoff).distinct()
        symbols = [r[0] for r in (await self._session.execute(stmt)).all()]
        return {"window_minutes": minutes, "distinct_symbols_with_signals": len(symbols), "symbols": symbols}

    async def _tool_get_open_position(self, trade_id: int) -> dict:
        """Текущее состояние своей сделки — открытой ИЛИ ещё не исполненного
        лимитника на вход (status='pending'). Для pending PnL/TP/SL не имеют
        смысла (entry_price — это ЦЕНА ЛИМИТНИКА, не факт входа), поэтому
        для них отдаём дистанцию до исполнения и остаток таймаута вместо этого."""
        trade = await self._session.get(Trade, trade_id)
        if not trade:
            return {"error": "trade not found"}
        now = datetime.now(tz=timezone.utc)
        current_price = await self._current_price(trade.symbol)

        if trade.status == "pending":
            minutes_until_expiry = None
            if trade.pending_expires_at:
                remaining = (
                    trade.pending_expires_at.replace(tzinfo=timezone.utc) - now
                ).total_seconds() / 60
                minutes_until_expiry = round(max(remaining, 0.0), 1)
            return {
                "trade_id": trade.id,
                "symbol": trade.symbol,
                "direction": trade.direction,
                "status": trade.status,
                "limit_price": trade.entry_price,
                "quantity": trade.quantity,
                "current_price": current_price,
                "distance_to_fill_pct": (
                    round((current_price / trade.entry_price - 1) * 100, 2)
                    if current_price and trade.entry_price else None
                ),
                "minutes_until_expiry": minutes_until_expiry,
            }

        age_hours = (now - trade.entry_time.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        sl_price = trade.current_sl_price
        tp_price = trade.current_tp_price
        if self._trading_config:
            if sl_price is None:
                sl_price = trade.entry_price * (1 - self._trading_config.stop_loss_pct / 100)
            if tp_price is None:
                sl_distance = trade.entry_price * (self._trading_config.stop_loss_pct / 100)
                tp_price = trade.entry_price + sl_distance * self._trading_config.risk_reward_ratio

        return {
            "trade_id": trade.id,
            "symbol": trade.symbol,
            "direction": trade.direction,
            "status": trade.status,
            "entry_price": trade.entry_price,
            "quantity": trade.quantity,
            "age_hours": round(age_hours, 2),
            "partial_closed": trade.partial_closed,
            "current_price": current_price,
            "current_sl_price": sl_price,
            "tp_price": tp_price,
            "hold_already_extended_hours": trade.llm_hold_extension_total_hours or 0.0,
            "unrealized_pnl_pct": (
                round((current_price / trade.entry_price - 1) * 100, 2)
                if current_price and trade.entry_price else None
            ),
        }

    async def _tool_get_recent_agent_decisions(self, trade_id: int, limit: int = 3) -> dict:
        """Прошлые решения агента по этой конкретной сделке (verdict + reasoning +
        когда) — континуити для сопровождения: видно, что уже решалось и почему,
        без необходимости держать одну LLM-сессию открытой все время жизни сделки."""
        stmt = (
            select(AgentDecision)
            .where(AgentDecision.trade_id == trade_id, AgentDecision.kind == "reeval")
            .order_by(desc(AgentDecision.timestamp))
            .limit(limit)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return {
            "trade_id": trade_id,
            "past_decisions": [
                {
                    "timestamp": r.timestamp.isoformat(),
                    "verdict": r.verdict,
                    "reasoning": r.reasoning,
                    "applied": r.applied,
                }
                for r in rows
            ],
        }


def build_strategy_briefing(
    strategy_config: StrategyConfig | None, trading_config: TradingConfig | None
) -> str:
    """Динамический блок с фактическими параметрами стратегии — собирается из
    живого конфига, а не хардкодится текстом (иначе разойдётся при правке
    config.yaml). Вызывается через scripts/agent_briefing.py, оркестратор
    вставляет вывод в промпт сабагенту при каждом диспетче. См. AGENTS.md,
    разделы 'Логика стратегий'/'Управление позициями'."""
    lines = ["### Параметры стратегии и аккаунта (актуальный конфиг — не переоткрывай эти проверки, они уже сделаны)"]

    if strategy_config:
        sc = strategy_config
        excl = ", ".join(sc.exclude_coins) or "—"
        lines.append(
            f"- Детектор (long-only, альткоин-перпетуалы Bybit, мин. суточный объём "
            f"${sc.min_volume_usdt:,.0f}, исключены: {excl}): сигнал уже подтверждён "
            f"объёмом ≥x{sc.volume_surge_mult} от нормы за {sc.sustain_bars} свечей подряд, "
            f"наклон OI ≥{sc.oi_slope_min_pct}%, рост цены {sc.price_growth_min_pct}–"
            f"{sc.price_growth_max_pct}% за окно, плюс антиспайк/антидамп и exhaustion-фильтры "
            f"(защита от входа на уже прошедшем хвосте пампа)."
        )

    if trading_config:
        tc = trading_config
        pullback = (
            f", вход лимитником на откате {tc.pending_entry_pullback_pct}% от сигнальной цены "
            f"(таймаут {tc.pending_entry_timeout_minutes:.0f} мин)"
            if tc.pending_entry_pullback_pct > 0 else ""
        )
        tp_pct = tc.stop_loss_pct * tc.risk_reward_ratio
        lines.append(
            f"- Риск/исполнение на ЭТОМ (отдельном) аккаунте: риск {tc.risk_per_trade_pct}% "
            f"от депозита на сделку, SL {tc.stop_loss_pct}% от входа, TP на "
            f"{tc.risk_reward_ratio}x риска (~{tp_pct:.1f}% от входа), плечо {tc.leverage}x, "
            f"частичная фиксация 50% на {tc.partial_close_pct}% пути до TP (стоп переводится "
            f"в безубыток), макс. удержание {tc.max_hold_hours}ч{pullback}."
        )
        lines.append(
            f"- Circuit Breaker уже применяется независимо от тебя: после "
            f"{tc.circuit_breaker_loss_streak_reduce} убытков подряд размер позиции снижается "
            f"до {tc.circuit_breaker_reduce_mult_pct:.0f}%, после "
            f"{tc.circuit_breaker_loss_streak_stop} — торговля останавливается на "
            f"{tc.circuit_breaker_stop_minutes} мин."
        )

    lines.append(
        "- Рыночный режим уже отфильтрован до тебя: новые входы не открываются в risk_off "
        "и в cautious при Supertrend=red (детали текущего режима — инструмент get_market_context)."
    )
    lines.append(
        "- Известная проблема, которую твой анализ должен смягчать: детектор по конструкции "
        "подтверждает сетап только после нескольких минут уже растущего движения — "
        "формальный вход часто оказывается близко к локальному пику движения."
    )
    return "\n".join(lines)
