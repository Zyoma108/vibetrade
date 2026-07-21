"""ИИ-агент: LLM (Claude) с tool-calling оценивает сигналы пампа и решает,
открывать ли сделку (evaluate_entry) и что делать с уже открытой (evaluate_position).

Работает ТОЛЬКО в рамках отдельного аккаунта биржи (source='agent' в Trade) —
не влияет на алгоритмическую торговлю ни при каких обстоятельствах. См. AGENTS.md,
раздел "ИИ-режим".

Fail-open (вход) / fail-safe (сопровождение): любая ошибка или таймаут LLM-вызова
не должны блокировать работу бота — см. PositionManager, где вызывается этот класс.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.agent.tools import ENTRY_TOOLS, FINAL_TOOL_SCHEMAS, REEVAL_TOOLS, AgentToolkit
from src.analytics.base import Signal
from src.analytics.data_provider import CandleCache
from src.analytics.market_context import MarketContext
from src.config import AgentConfig, StrategyConfig, TradingConfig
from src.connectors.exchange import ExchangeConnector
from src.storage.models import Trade

logger = logging.getLogger(__name__)

# Версия системных промптов — логируется с каждым решением (AgentDecision.agent_version),
# чтобы позже можно было сопоставить качество решений с конкретной редакцией инструкций.
AGENT_VERSION = "v1"

ENTRY_SYSTEM_PROMPT = """Ты — риск-аналитик поверх алгоритмического long-only детектора \
пампов криптовалютных перпетуалов на Bybit.

Алгоритм уже проверил объём/OI/цену и сгенерировал сигнал — твоя задача не переоткрывать \
эти проверки, а оценить контекст, который алгоритм не видит: перекупленность (funding), \
исполнимость (стакан), историю монеты (повторные пампы), рыночный контекст и старшие \
таймфреймы (уровни поддержки/сопротивления).

Используй инструменты в любом порядке и количестве в пределах лимита. Одобряй вход, если \
нет явных признаков того, что вход произойдёт на уже перегретом хвосте движения (высокий \
funding против направления, тонкий стакан, монета — известный памп-дамп рецидивист, \
движение уже упёрлось в сильное старшее сопротивление). При отсутствии явных красных \
флагов — одобряй, не будь избыточно консервативным: цель — отсеивать худшие случаи, а не \
находить идеальные входы.

Заверши анализ вызовом submit_entry_decision."""

REEVAL_SYSTEM_PROMPT = """Ты сопровождаешь уже открытую long-позицию, взятую по сигналу \
алгоритмического детектора пампов. У позиции уже есть биржевой стоп-лосс и тейк-профит — \
они остаются главной защитой независимо от твоего решения.

Доступные действия:
- hold — ничего не менять, дефолт при отсутствии значимых новых данных.
- tighten_sl — подтянуть стоп ближе к текущей цене (например, если старший таймфрейм \
показывает близкое сопротивление или движение выдыхается). Новый стоп ОБЯЗАН быть строже \
текущего — ослаблять стоп нельзя, система отклонит такую попытку.
- extend_hold — продлить время удержания, если движение выглядит здоровым, а видимого \
катализатора для выхода нет, но время по счётчику вот-вот истечёт.
- close — закрыть досрочно, если данные явно говорят о развороте (funding резко перегрет \
против позиции, стакан истончился, аномальный объём на продажу), не дожидаясь штатного TP/SL.

Используй инструменты, чтобы понять актуальную динамику. Заверши вызовом submit_reeval_decision."""


@dataclass
class EntryVerdict:
    approved: bool
    reasoning: str
    tool_trace: list[dict] = field(default_factory=list)
    latency_ms: int = 0
    failed: bool = False  # True — решение не получено (таймаут/ошибка/бюджет), вызывающий код fail-open


@dataclass
class ReevalVerdict:
    action: str  # hold / tighten_sl / extend_hold / close
    reasoning: str
    new_sl_price: float | None = None
    extend_hours: float | None = None
    tool_trace: list[dict] = field(default_factory=list)
    latency_ms: int = 0
    failed: bool = False  # True — решение не получено, вызывающий код fail-safe (hold)


class DecisionAgent:
    """Один экземпляр на приложение. Держит Anthropic-клиент и коннектор
    ОТДЕЛЬНОГО аккаунта ИИ-режима (передаётся вызывающим кодом)."""

    def __init__(
        self,
        config: AgentConfig,
        connector: ExchangeConnector,
        candle_cache: CandleCache | None = None,
        market_ctx: MarketContext | None = None,
        trading_config: TradingConfig | None = None,
        strategy_config: StrategyConfig | None = None,
    ):
        self.config = config
        self._connector = connector
        self._candle_cache = candle_cache
        self._market_ctx = market_ctx
        self._trading_config = trading_config
        self._strategy_config = strategy_config
        self._client = None
        if config.enabled:
            import anthropic
            self._client = anthropic.AsyncAnthropic()  # ANTHROPIC_API_KEY из окружения
        self._calls_today = 0
        self._calls_reset_at = datetime.now(tz=timezone.utc) + timedelta(days=1)

        # Конфиг не меняется на лету (нет hot-reload) — строим один раз при старте,
        # чтобы промпт всегда отражал РЕАЛЬНЫЕ пороги детектора и риск-параметры,
        # а не текст, который может разойтись с конфигом при правке последнего.
        self._strategy_briefing = self._build_strategy_briefing()

    def _build_strategy_briefing(self) -> str:
        """Динамический блок с фактическими параметрами стратегии — собирается из
        живого конфига, а не хардкодится текстом (иначе разойдётся при правке
        config.yaml). См. AGENTS.md, разделы 'Логика стратегий'/'Управление позициями'."""
        sc, tc = self._strategy_config, self._trading_config
        lines = ["### Параметры стратегии и аккаунта (актуальный конфиг — не переоткрывай эти проверки, они уже сделаны)"]

        if sc:
            excl = ", ".join(sc.exclude_coins) or "—"
            lines.append(
                f"- Детектор (long-only, альткоин-перпетуалы Bybit, мин. суточный объём "
                f"${sc.min_volume_usdt:,.0f}, исключены: {excl}): сигнал уже подтверждён "
                f"объёмом ≥x{sc.volume_surge_mult} от нормы за {sc.sustain_bars} свечей подряд, "
                f"наклон OI ≥{sc.oi_slope_min_pct}%, рост цены {sc.price_growth_min_pct}–"
                f"{sc.price_growth_max_pct}% за окно, плюс антиспайк/антидамп и exhaustion-фильтры "
                f"(защита от входа на уже прошедшем хвосте пампа)."
            )

        if tc:
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

    def _check_budget(self) -> bool:
        now = datetime.now(tz=timezone.utc)
        if now >= self._calls_reset_at:
            self._calls_today = 0
            self._calls_reset_at = now + timedelta(days=1)
        if self._calls_today >= self.config.daily_call_budget:
            logger.warning("Agent: дневной лимит вызовов LLM исчерпан")
            return False
        self._calls_today += 1
        return True

    async def evaluate_entry(self, session: AsyncSession, signal: Signal) -> EntryVerdict:
        if not self._check_budget():
            return EntryVerdict(approved=True, reasoning="daily_budget_exhausted", failed=True)

        toolkit = AgentToolkit(session, self._connector, self._candle_cache, self._market_ctx, self._trading_config)
        user_prompt = (
            f"Новый сигнал пампа: {signal.symbol}, направление {signal.direction}, "
            f"уверенность {signal.confidence}%.\n{signal.message}\n\n"
            "Собери данные инструментами и реши: одобрить вход или отклонить. "
            "Заверши вызовом submit_entry_decision."
        )
        system_prompt = f"{ENTRY_SYSTEM_PROMPT}\n\n{self._strategy_briefing}"
        start = time.monotonic()
        try:
            result = await asyncio.wait_for(
                self._run_loop(user_prompt, system_prompt, ENTRY_TOOLS, toolkit, "submit_entry_decision"),
                timeout=self.config.decision_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Agent: таймаут решения по входу {signal.symbol} — fail-open (approve)")
            return EntryVerdict(approved=True, reasoning="timeout", tool_trace=toolkit.calls, failed=True)
        except Exception as e:
            logger.exception(f"Agent: ошибка решения по входу {signal.symbol} — fail-open (approve)")
            return EntryVerdict(approved=True, reasoning=f"error: {e}", tool_trace=toolkit.calls, failed=True)

        latency_ms = int((time.monotonic() - start) * 1000)
        if result is None:
            logger.warning(f"Agent: решение по входу {signal.symbol} не получено — fail-open (approve)")
            return EntryVerdict(approved=True, reasoning="no_decision_reached", tool_trace=toolkit.calls, failed=True)

        return EntryVerdict(
            approved=bool(result.get("approve", True)),
            reasoning=result.get("reasoning", ""),
            tool_trace=toolkit.calls,
            latency_ms=latency_ms,
        )

    async def evaluate_position(self, session: AsyncSession, trade: Trade) -> ReevalVerdict:
        if not self._check_budget():
            return ReevalVerdict(action="hold", reasoning="daily_budget_exhausted", failed=True)

        toolkit = AgentToolkit(session, self._connector, self._candle_cache, self._market_ctx, self._trading_config)
        age_hours = (
            datetime.now(tz=timezone.utc) - trade.entry_time.replace(tzinfo=timezone.utc)
        ).total_seconds() / 3600
        user_prompt = (
            f"Открытая сделка: {trade.symbol}, направление {trade.direction}, "
            f"вход ${trade.entry_price:.6f}, возраст {age_hours:.1f}ч, "
            f"частично закрыта: {trade.partial_closed}.\n"
            "Реши: держать без изменений, подтянуть стоп, продлить время удержания, "
            "или закрыть досрочно. Вызови get_open_position, чтобы узнать текущий стоп/тейк "
            "и нереализованный PnL перед решением. Заверши вызовом submit_reeval_decision.\n"
            f"(trade_id для инструмента get_open_position: {trade.id})"
        )
        system_prompt = f"{REEVAL_SYSTEM_PROMPT}\n\n{self._strategy_briefing}"
        start = time.monotonic()
        try:
            result = await asyncio.wait_for(
                self._run_loop(user_prompt, system_prompt, REEVAL_TOOLS, toolkit, "submit_reeval_decision"),
                timeout=self.config.decision_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Agent: таймаут переоценки {trade.symbol} — fail-safe (hold)")
            return ReevalVerdict(action="hold", reasoning="timeout", tool_trace=toolkit.calls, failed=True)
        except Exception as e:
            logger.exception(f"Agent: ошибка переоценки {trade.symbol} — fail-safe (hold)")
            return ReevalVerdict(action="hold", reasoning=f"error: {e}", tool_trace=toolkit.calls, failed=True)

        latency_ms = int((time.monotonic() - start) * 1000)
        if result is None:
            logger.warning(f"Agent: переоценка {trade.symbol} не завершена — fail-safe (hold)")
            return ReevalVerdict(action="hold", reasoning="no_decision_reached", tool_trace=toolkit.calls, failed=True)

        return ReevalVerdict(
            action=result.get("action", "hold"),
            reasoning=result.get("reasoning", ""),
            new_sl_price=result.get("new_sl_price"),
            extend_hours=result.get("extend_hours"),
            tool_trace=toolkit.calls,
            latency_ms=latency_ms,
        )

    async def _run_loop(
        self,
        user_prompt: str,
        system_prompt: str,
        tools: list[dict],
        toolkit: AgentToolkit,
        final_tool_name: str,
    ) -> dict | None:
        messages: list[dict] = [{"role": "user", "content": user_prompt}]
        all_tools = tools + [FINAL_TOOL_SCHEMAS[final_tool_name]]

        for _ in range(self.config.max_tool_calls_per_decision):
            response = await self._client.messages.create(  # type: ignore[union-attr]
                model=self.config.model,
                max_tokens=1500,
                system=system_prompt,
                tools=all_tools,
                messages=messages,
            )
            messages.append({"role": "assistant", "content": response.content})
            tool_uses = [b for b in response.content if b.type == "tool_use"]
            if not tool_uses:
                return None  # модель не завершила решение и не запросила инструмент

            final_call = next((tu for tu in tool_uses if tu.name == final_tool_name), None)
            if final_call:
                return final_call.input

            tool_results = []
            for tu in tool_uses:
                output = await toolkit.dispatch(tu.name, tu.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": json.dumps(output, default=str),
                })
            messages.append({"role": "user", "content": tool_results})

        return None  # превышен лимит вызовов инструментов
