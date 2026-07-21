"""
Прогон стратегии на исторических данных.

Использование:
    python -m src.backtest.runner
    python -m src.backtest.runner --db data/trading_bot.db --config config/test-config.yaml
"""

import argparse
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.detector import SetupDetector
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct
from src.config import Settings
from src.storage.models import Candle, MarketContextSnapshot, OpenInterest

logger = logging.getLogger(__name__)

BACKTEST_DB = Path("data/backtest.db")
CYCLE_DELAY_BARS = 3


# ---------------------------------------------------------------------------
# Virtual position (без биржи)
# ---------------------------------------------------------------------------

class SimPosition:
    __slots__ = (
        "symbol", "entry_price", "entry_time", "quantity",
        "tp_price", "sl_price", "partial_closed", "partial_pnl",
        "closed", "exit_price", "exit_time", "pnl", "exit_reason", "fee",
    )

    def __init__(self, symbol: str, entry_price: float, entry_time: datetime,
                 quantity: float, tp_price: float, sl_price: float, fee: float = 0.0):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_time = entry_time
        self.quantity = quantity
        self.tp_price = tp_price
        self.sl_price = sl_price
        self.partial_closed = False
        self.partial_pnl = 0.0
        self.closed = False
        self.exit_price = 0.0
        self.exit_time: datetime | None = None
        self.pnl = 0.0
        self.exit_reason = ""
        self.fee = fee  # накопленная комиссия по всем "ногам" сделки


def _bar(rows, idx):
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


def _fee(cfg, notional: float, taker: bool) -> float:
    """Комиссия биржи за одну "ногу" сделки (см. PositionManager._fee).
    taker=True — market-ордер (вход, TP/SL/time-exit, fallback partial).
    taker=False — резервный reduce-only лимитник частичной фиксации (maker)."""
    rate = cfg.taker_fee_pct if taker else cfg.maker_fee_pct
    return notional * (rate / 100)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_backtest(
    config_path: str = "config/config.yaml",
    db_path: str = "data/backtest.db",
    has_oi: bool = True
) -> dict:
    """Запустить бэктест и вернуть статистику."""
    settings = Settings.from_yaml(config_path)
    db_path = Path(db_path)

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Загружаем все свечи в память
    async with session_factory() as session:
        stmt = (
            select(Candle.symbol, Candle.timestamp, Candle.open, Candle.high,
                   Candle.low, Candle.close, Candle.volume)
            .order_by(Candle.symbol, Candle.timestamp)
        )
        result = await session.execute(stmt)
        rows = result.all()

    symbols: dict[str, list] = {}
    for r in rows:
        symbols.setdefault(r[0], []).append(r)

    all_timestamps = sorted(set(r[1] for r in rows))
    if not all_timestamps:
        logger.error("Нет данных для бэктеста")
        return {}

    start_time = all_timestamps[0]
    end_time = all_timestamps[-1]
    logger.info(f"БД: {db_path} | OI: {'✅' if has_oi else '❌'}")
    logger.info(f"Символов: {len(symbols)} | Период: {start_time} → {end_time}")
    logger.info(f"Временных срезов: {len(all_timestamps)}")

    detector = SetupDetector(settings.strategy, timeframe=settings.collectors.timeframe)
    cfg = settings.trading

    positions: list[SimPosition] = []
    closed_trades: list[SimPosition] = []
    signals_count = 0

    # Circuit Breaker state
    cb_losses = 0
    cb_stop_until: datetime | None = None

    # Кеш OI-данных (exchange, symbol) -> [(timestamp, value), ...]
    oi_cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    if has_oi:
        async with session_factory() as session:
            oi_stmt = select(OpenInterest.exchange, OpenInterest.symbol,
                             OpenInterest.timestamp, OpenInterest.value).order_by(
                OpenInterest.exchange, OpenInterest.symbol, OpenInterest.timestamp)
            oi_result = await session.execute(oi_stmt)
            for ex, sym, ts, val in oi_result.all():
                oi_cache.setdefault((ex, sym), []).append((ts, val))
        logger.info(f"Загружено OI: {len(oi_cache)} монет")

    # Загружаем снимки рыночного контекста
    mc_snapshots: list[tuple[datetime, "MarketContextSnapshot"]] = []
    try:
        async with session_factory() as session:
            mc_stmt = (
                select(MarketContextSnapshot)
                .order_by(MarketContextSnapshot.timestamp)
            )
            mc_result = await session.execute(mc_stmt)
            for row in mc_result.scalars().all():
                mc_snapshots.append((row.timestamp, row))
        logger.info(f"Загружено снимков MarketContext: {len(mc_snapshots)}")
    except Exception:
        logger.warning(
            "MarketContext не загружен — фильтрация по режиму отключена"
        )

    # Проходим по каждому временному срезу
    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < detector.config.baseline_bars + detector.config.sustain_bars:
            continue

        # Определяем режим рынка на этот момент времени
        regime = "unknown"
        st_color = "green"
        for mc_ts, mc_row in reversed(mc_snapshots):
            if mc_ts <= ts:
                regime = mc_row.regime
                st_color = mc_row.supertrend_color
                break
        if regime == "risk_off" or (regime == "cautious" and st_color == "red"):
            continue

        # Проверяем открытые позиции на TP/SL/время
        for pos in list(positions):
            if pos.closed:
                continue

            sym_data = symbols.get(pos.symbol)
            if not sym_data:
                continue

            current_bar = None
            for r in sym_data:
                if r[1] == ts:
                    current_bar = r
                    break
            if not current_bar:
                continue

            high = current_bar[3]
            low = current_bar[4]
            close = current_bar[5]

            # Частичное закрытие (всегда включено)
            if not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    cfg.partial_close_pct / 100)
                if high >= trigger:
                    close_qty = pos.quantity / 2
                    partial_pnl = (trigger - pos.entry_price) * close_qty
                    pos.quantity -= close_qty
                    pos.partial_closed = True
                    pos.partial_pnl = partial_pnl
                    pos.sl_price = pos.entry_price
                    # Резервный лимитник частичной фиксации — maker
                    pos.fee += _fee(cfg, trigger * close_qty, taker=False)
                    continue

            # TP
            if high >= pos.tp_price:
                pos.exit_price = pos.tp_price
                pos.exit_time = ts
                pos.exit_reason = "tp"
                pos.fee += _fee(cfg, pos.tp_price * pos.quantity, taker=True)
                pos.pnl = (pos.tp_price - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                # Circuit Breaker: сброс счётчика при прибыли
                if cfg.circuit_breaker_enabled:
                    cb_losses = 0
                    cb_stop_until = None
                continue

            # SL
            if low <= pos.sl_price:
                pos.exit_price = pos.sl_price
                pos.exit_time = ts
                pos.exit_reason = "sl"
                pos.fee += _fee(cfg, pos.sl_price * pos.quantity, taker=True)
                pos.pnl = (pos.sl_price - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                # Circuit Breaker: увеличиваем счётчик убытков
                if cfg.circuit_breaker_enabled:
                    cb_losses += 1
                continue

            # Выход по времени
            age = (ts - pos.entry_time).total_seconds() / 3600
            if age >= cfg.max_hold_hours:
                pos.exit_price = close
                pos.exit_time = ts
                pos.exit_reason = "time"
                pos.fee += _fee(cfg, close * pos.quantity, taker=True)
                pos.pnl = (close - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                # Circuit Breaker: по PnL
                if cfg.circuit_breaker_enabled:
                    if pos.pnl > 0:
                        cb_losses = 0
                        cb_stop_until = None
                    else:
                        cb_losses += 1

        # Ищем сетапы только в «циклы сканирования»
        if ts_idx % CYCLE_DELAY_BARS != 0:
            continue
        if len(positions) >= cfg.max_positions:
            continue

        # Circuit Breaker: проверка полной остановки
        cb_mult = 1.0
        if cfg.circuit_breaker_enabled:
            if cb_stop_until is not None:
                if ts < cb_stop_until:
                    continue
                # Таймер истёк — сбрасываем
                cb_stop_until = None
                cb_losses = 0
            if cb_losses >= cfg.circuit_breaker_loss_streak_stop:
                cb_stop_until = ts + timedelta(minutes=cfg.circuit_breaker_stop_minutes)
                logger.warning(
                    f"Circuit Breaker: {cb_losses} убытков подряд → "
                    f"ПОЛНАЯ ОСТАНОВКА до {cb_stop_until}"
                )
                continue
            if cb_losses >= cfg.circuit_breaker_loss_streak_reduce:
                cb_mult = cfg.circuit_breaker_reduce_mult_pct / 100.0

        # Применяем множитель рыночного режима к детектору
        if regime == "cautious":
            increase_pct = detector.config.cautious_volume_surge_mult_increase_pct
            detector.apply_regime_multiplier(1.0 + increase_pct / 100.0)
        else:
            detector.apply_regime_multiplier(1.0)

        for sym, sym_data in symbols.items():
            if len(positions) >= cfg.max_positions:
                break

            bar_idx = -1
            for i, r in enumerate(sym_data):
                if r[1] == ts:
                    bar_idx = i
                    break
            if bar_idx < 0:
                continue

            need_bars = detector.config.baseline_bars + detector.config.sustain_bars
            if bar_idx < need_bars:
                continue

            base = sym.split("/")[0].upper()
            if base in getattr(detector, '_exclude_coins', set()):
                continue
            if any(p.symbol == sym for p in positions):
                continue

            cooldown_cutoff = ts - timedelta(hours=cfg.cooldown_hours)
            if cfg.cooldown_hours > 0 and any(
                t.symbol == sym and t.exit_time and t.exit_time >= cooldown_cutoff
                for t in closed_trades
            ):
                continue

            candle_slice = []
            for j in range(bar_idx - need_bars - 10, bar_idx + 1):
                bar = _bar(sym_data, j)
                if bar:
                    candle_slice.append({
                        "open": bar[2], "high": bar[3],
                        "low": bar[4], "close": bar[5], "volume": bar[6],
                    })

            if len(candle_slice) < need_bars:
                continue

            # Volume + price checks
            if not detector.check_volume_pattern(candle_slice):
                continue
            direction = detector.check_price_trend(candle_slice)
            if direction != "long":
                continue

            # OI check (если есть данные)
            if has_oi:
                oi_pass = False
                for ex in ("bybit", "binance"):
                    key = (ex, sym)
                    if key not in oi_cache:
                        continue
                    # Берём OI точки до текущего timestamp
                    oi_points = [v for t, v in oi_cache[key] if t <= ts]
                    if len(oi_points) < OI_TREND_BARS:
                        continue
                    oi_vals = np.array(oi_points[-OI_TREND_BARS:])
                    slope_pct = calculate_oi_slope_pct(oi_vals)
                    if slope_pct is not None and slope_pct >= detector.config.oi_slope_min_pct:
                        oi_pass = True
                        break
                if not oi_pass:
                    continue

            signals_count += 1

            signal_price = candle_slice[-1]["close"]
            # Реальный вход рыночным ордером хуже цены сигнала на величину проскальзывания
            entry_price = signal_price * (1 + cfg.backtest_slippage_pct / 100)

            # TP/SL: фиксированные проценты от цены входа (после проскальзывания)
            sl_distance = entry_price * (cfg.stop_loss_pct / 100)
            tp_distance = sl_distance * cfg.risk_reward_ratio

            # Бюджет риска: % от виртуального депозита $1000
            virtual_balance = 1000.0
            risk_budget = virtual_balance * (cfg.risk_per_trade_pct / 100) * cb_mult
            qty = risk_budget / sl_distance
            tp = entry_price + tp_distance
            sl = entry_price - sl_distance
            entry_fee = _fee(cfg, qty * entry_price, taker=True)

            pos = SimPosition(
                symbol=sym, entry_price=entry_price, entry_time=ts,
                quantity=qty, tp_price=tp, sl_price=sl, fee=entry_fee,
            )
            positions.append(pos)

    # Закрываем оставшиеся позиции
    for pos in positions:
        sym_data = symbols.get(pos.symbol)
        if sym_data:
            last_close = sym_data[-1][5]
            pos.exit_price = last_close
            pos.exit_time = end_time
            pos.exit_reason = "eod"
            pos.fee += _fee(cfg, last_close * pos.quantity, taker=True)
            pos.pnl = (last_close - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
        pos.closed = True
        closed_trades.append(pos)

    # Статистика
    wins = sum(1 for t in closed_trades if t.pnl > 0)
    losses = sum(1 for t in closed_trades if t.pnl <= 0)
    total_pnl = sum(t.pnl for t in closed_trades)
    win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0

    tp_wins = sum(1 for t in closed_trades if t.exit_reason == "tp")
    sl_losses = sum(1 for t in closed_trades if t.exit_reason == "sl")
    time_exits = sum(1 for t in closed_trades if t.exit_reason == "time")
    partials = sum(1 for t in closed_trades if t.partial_closed)
    total_fees = sum(t.fee for t in closed_trades)

    await engine.dispose()

    return {
        "signals": signals_count,
        "trades": len(closed_trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(closed_trades), 2) if closed_trades else 0,
        "total_fees": round(total_fees, 2),
        "avg_fee": round(total_fees / len(closed_trades), 4) if closed_trades else 0,
        "tp_wins": tp_wins,
        "sl_losses": sl_losses,
        "time_exits": time_exits,
        "partials": partials,
        "best": max(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "worst": min(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "period": f"{start_time} → {end_time}",
        "trades_list": closed_trades,
        "has_oi": has_oi,
        "has_mc": len(mc_snapshots) > 0,
    }


def _load_live_stats(db_path: str) -> dict | None:
    """Загрузить статистику реальных сделок из БД."""
    import sqlite3

    try:
        db = sqlite3.connect(db_path)
    except Exception:
        return None

    trades = db.execute(
        "SELECT symbol, direction, entry_price, exit_price, "
        "entry_time, exit_time, pnl, status, partial_closed, partial_pnl "
        "FROM trades WHERE status = 'closed' ORDER BY exit_time"
    ).fetchall()

    if not trades:
        db.close()
        return None

    wins = sum(1 for t in trades if (t[6] or 0) > 0)
    losses = sum(1 for t in trades if (t[6] or 0) <= 0)
    total_pnl = sum(t[6] or 0 for t in trades)
    win_rate = wins / len(trades) * 100 if trades else 0

    # Диапазон дат
    entry_times = [t[4] for t in trades if t[4]]
    exit_times = [t[5] for t in trades if t[5]]
    period = ""
    if entry_times and exit_times:
        period = f"{min(entry_times)[:19]} → {max(exit_times)[:19]}"

    # Причины выхода (по данным сигналов: tp, sl, time)
    signals = db.execute(
        "SELECT s.symbol, s.timestamp, s.missed_reason "
        "FROM signals s ORDER BY s.timestamp"
    ).fetchall()

    sent_count = sum(1 for s in signals if s[2] is None)
    missed_count = sum(1 for s in signals if s[2] is not None)
    missed_reasons = {}
    for s in signals:
        if s[2]:
            missed_reasons[s[2]] = missed_reasons.get(s[2], 0) + 1

    db.close()

    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(trades), 2) if trades else 0,
        "period": period,
        "signals_total": len(signals),
        "signals_sent": sent_count,
        "signals_missed": missed_count,
        "missed_reasons": missed_reasons,
        "best": max(trades, key=lambda t: t[6] or 0) if trades else None,
        "worst": min(trades, key=lambda t: t[6] or 0) if trades else None,
    }


def _print_comparison(bt: dict, live: dict | None) -> None:
    """Вывести сравнение бэктест ↔ реальная торговля."""
    print("\n" + "=" * 80)
    print(f"  {'':30s} {'БЭКТЕСТ':>22s} {'РЕАЛЬНАЯ ТОРГОВЛЯ':>22s}")
    print("=" * 80)

    rows = [
        ("Сделок", str(bt["trades"]), str(live["trades"]) if live else "—"),
        ("Плюс / Минус",
         f"{bt['wins']} / {bt['losses']}",
         f"{live['wins']} / {live['losses']}" if live else "—"),
        ("Win rate",
         f"{bt['win_rate']}%",
         f"{live['win_rate']}%" if live else "—"),
        ("Total PnL",
         f"${bt['total_pnl']:+.2f}",
         f"${live['total_pnl']:+.2f}" if live else "—"),
        ("Средний PnL",
         f"${bt['avg_pnl']:+.2f}",
         f"${live['avg_pnl']:+.2f}" if live else "—"),
        ("TP / SL / Time",
         f"{bt['tp_wins']} / {bt['sl_losses']} / {bt['time_exits']}",
         "—"),
        ("Частичных закр.",
         str(bt["partials"]),
         "—"),
        ("Период",
         bt["period"][:35] if len(bt["period"]) > 35 else bt["period"],
         live["period"][:35] if live and live["period"] and len(live["period"]) > 35 else (live["period"] if live else "—")),
    ]

    for label, bt_val, live_val in rows:
        print(f"  {label:<30s} {bt_val:>22s} {live_val:>22s}")

    print("=" * 80)

    if live and live.get("signals_total"):
        print("\n  Конвейер сигналов (реальная торговля):")
        print(f"    Всего сигналов: {live['signals_total']}")
        print(f"    Отправлено:     {live['signals_sent']}")
        print(f"    Пропущено:      {live['signals_missed']}")
        if live["missed_reasons"]:
            for reason, count in sorted(
                live["missed_reasons"].items(), key=lambda x: -x[1]
            ):
                print(f"      - {reason}: {count}")


def _print_trade_list(trades: list, title: str, max_show: int = 20) -> None:
    """Вывести список сделок."""
    print(f"\n  {title} (последние {min(len(trades), max_show)}):")
    for t in trades[-max_show:]:
        emoji = "✅" if t.exit_reason == "tp" else (
            "🛑" if t.exit_reason == "sl" else "⏰"
        )
        pnl_pct = (t.exit_price / t.entry_price - 1) * 100
        print(
            f"  {emoji} {t.symbol:25s} "
            f"вход=${t.entry_price:.6f} выход=${t.exit_price:.6f} "
            f"PnL=${t.pnl:+.2f} ({pnl_pct:+.1f}%)  [{t.exit_reason}]"
        )


def main():
    parser = argparse.ArgumentParser(description="Бэктест торговой стратегии")
    parser.add_argument("--config", type=str, default="config/config.yaml")
    parser.add_argument("--db", type=str, default="data/backtest.db")
    parser.add_argument("--has_oi", type=bool, default=True)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = asyncio.run(run_backtest(args.config, args.db, args.has_oi))

    if not result:
        print("Нет данных")
        return

    # Загружаем реальные сделки из той же БД (если есть)
    live = _load_live_stats(args.db)

    # Сводка бэктеста
    print("\n" + "=" * 60)
    print("  РЕЗУЛЬТАТЫ БЭКТЕСТА")
    print("=" * 60)
    print(f"  OI проверка:        {'✅ Да' if result['has_oi'] else '❌ Нет'}")
    print(f"  MarketContext:      {'✅ Да' if result.get('has_mc', False) else '❌ Нет'}")
    print(f"  Период:            {result['period']}")
    print(f"  Сигналов:          {result['signals']}")
    print(f"  Сделок:            {result['trades']}")
    print(f"  Плюс / Минус:      {result['wins']} / {result['losses']}")
    print(f"  Win rate:          {result['win_rate']}%")
    print(f"  Total PnL:         ${result['total_pnl']:+.2f} (net of fees)")
    print(f"  Средний PnL:       ${result['avg_pnl']:+.2f}")
    print(f"  Комиссии всего:    ${result['total_fees']:.2f} (сред. ${result['avg_fee']:.4f}/сделку)")
    print(f"  TP: {result['tp_wins']} | SL: {result['sl_losses']} | Time: {result['time_exits']}")
    print(f"  Частичных закрытий: {result['partials']}")
    if result["best"]:
        print(f"  Лучшая:  {result['best'].symbol} ${result['best'].pnl:+.2f}")
    if result["worst"]:
        print(f"  Худшая:  {result['worst'].symbol} ${result['worst'].pnl:+.2f}")
    print("=" * 60)

    # Сравнение с реальной торговлей
    if live:
        _print_comparison(result, live)

    _print_trade_list(result["trades_list"], "Сделки бэктеста")


if __name__ == "__main__":
    main()
