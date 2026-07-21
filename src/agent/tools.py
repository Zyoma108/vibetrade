"""Инструменты (tools) для ИИ-агента — только чтение данных, без побочных
эффектов на торговлю. Решения о входе/сопровождении принимает DecisionAgent,
исполнение — PositionManager (отдельный аккаунт, source='agent').

Каждый инструмент отдаёт агрегированные компактные метрики, а не сырые дампы
API — иначе контекст LLM быстро раздувается шумом (см. AGENTS.md, раздел
про ИИ-режим)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.analytics.data_provider import CandleCache
from src.analytics.market_context import MarketContext
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Candle, OpenInterest, Signal, Ticker, Trade

logger = logging.getLogger(__name__)


class AgentToolkit:
    """Один экземпляр на вызов агента (evaluate_entry/evaluate_position).
    Копит трейс вызовов инструментов в self.calls — для аудита в AgentDecision."""

    def __init__(
        self,
        session: AsyncSession,
        connector: ExchangeConnector,
        candle_cache: CandleCache | None = None,
        market_ctx: MarketContext | None = None,
    ):
        self._session = session
        self._connector = connector
        self._candle_cache = candle_cache
        self._market_ctx = market_ctx
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

    # ------------------------------------------------------------------
    # Data tools
    # ------------------------------------------------------------------

    async def _tool_get_symbol_snapshot(self, symbol: str, bars: int = 30) -> dict:
        """Последние N свечей текущего таймфрейма: диапазон цены, объём, % изменения."""
        exchange = self._connector.exchange_id
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
        exchange = self._connector.exchange_id
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
        цены BTC/OTHERS за 1ч и 4ч."""
        if not self._market_ctx or not self._market_ctx.ready:
            return {"error": "market context unavailable"}
        snap = self._market_ctx.get_snapshot()
        snap.pop("timestamp", None)
        return snap

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
        """Текущее состояние своей открытой сделки (только для сопровождения)."""
        trade = await self._session.get(Trade, trade_id)
        if not trade:
            return {"error": "trade not found"}
        now = datetime.now(tz=timezone.utc)
        age_hours = (now - trade.entry_time.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        current_price = await self._current_price(trade.symbol)
        return {
            "symbol": trade.symbol,
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "quantity": trade.quantity,
            "age_hours": round(age_hours, 2),
            "partial_closed": trade.partial_closed,
            "current_price": current_price,
            "unrealized_pnl_pct": (
                round((current_price / trade.entry_price - 1) * 100, 2)
                if current_price and trade.entry_price else None
            ),
        }


# ----------------------------------------------------------------------
# Anthropic tool schemas
# ----------------------------------------------------------------------

SHARED_TOOLS: list[dict] = [
    {
        "name": "get_symbol_snapshot",
        "description": "Последние N свечей текущего таймфрейма для монеты: диапазон цены, объём, % изменения.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "bars": {"type": "integer", "default": 30},
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "get_oi_trend",
        "description": "Тренд открытого интереса (OI) за последние N точек — рост OI вместе с объёмом означает новые деньги, а не перекладывание позиций.",
        "input_schema": {
            "type": "object",
            "properties": {"symbol": {"type": "string"}, "n_bars": {"type": "integer", "default": 10}},
            "required": ["symbol"],
        },
    },
    {
        "name": "get_funding_rate",
        "description": "Текущая funding rate по перпетуалу. Сильно положительная funding = лонги уже перегружены, повышенный риск лонг-сквиза.",
        "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]},
    },
    {
        "name": "get_order_book_summary",
        "description": "Агрегированная сводка стакана: спред и глубина в USD на ±0.5%/±1% от средней цены. Показывает исполнимость.",
        "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]},
    },
    {
        "name": "get_symbol_pump_history",
        "description": "Последние N сигналов по монете и их исход (сделка/пропуск/PnL) — история повторных пампов.",
        "input_schema": {
            "type": "object",
            "properties": {"symbol": {"type": "string"}, "limit": {"type": "integer", "default": 5}},
            "required": ["symbol"],
        },
    },
    {
        "name": "get_market_context",
        "description": "Текущий рыночный режим (risk_on/cautious/risk_off), тренд и изменение цены BTC/OTHERS.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_higher_timeframe_history",
        "description": "OHLC-свечи на старшем таймфрейме (напр. 4h/1d) для поиска уровней поддержки/сопротивления.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string", "default": "4h", "description": "например 1h, 4h, 1d"},
                "limit": {"type": "integer", "default": 90},
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "get_recent_signal_activity",
        "description": "Сколько разных монет дали сигнал за последние N минут — прокси секторальной ротации против изолированного пампа одной монеты.",
        "input_schema": {
            "type": "object",
            "properties": {"minutes": {"type": "integer", "default": 15}},
        },
    },
]

ENTRY_TOOLS: list[dict] = SHARED_TOOLS

REEVAL_TOOLS: list[dict] = SHARED_TOOLS + [
    {
        "name": "get_open_position",
        "description": "Текущее состояние открытой сделки: возраст, цена входа/текущая, нереализованный PnL%.",
        "input_schema": {"type": "object", "properties": {"trade_id": {"type": "integer"}}, "required": ["trade_id"]},
    },
]

SUBMIT_ENTRY_DECISION_TOOL: dict = {
    "name": "submit_entry_decision",
    "description": "Завершить анализ и вынести решение по входу в сделку.",
    "input_schema": {
        "type": "object",
        "properties": {
            "approve": {"type": "boolean", "description": "true = одобрить вход, false = отклонить"},
            "reasoning": {"type": "string", "description": "Краткое обоснование на основе собранных данных"},
        },
        "required": ["approve", "reasoning"],
    },
}

SUBMIT_REEVAL_DECISION_TOOL: dict = {
    "name": "submit_reeval_decision",
    "description": "Завершить переоценку открытой сделки и вынести решение.",
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["hold", "tighten_sl", "extend_hold", "close"]},
            "reasoning": {"type": "string"},
            "new_sl_price": {
                "type": "number",
                "description": "Только для action=tighten_sl — новая цена стопа (должна быть строже текущей)",
            },
            "extend_hours": {
                "type": "number",
                "description": "Только для action=extend_hold — на сколько часов продлить удержание",
            },
        },
        "required": ["action", "reasoning"],
    },
}

FINAL_TOOL_SCHEMAS: dict[str, dict] = {
    "submit_entry_decision": SUBMIT_ENTRY_DECISION_TOOL,
    "submit_reeval_decision": SUBMIT_REEVAL_DECISION_TOOL,
}
