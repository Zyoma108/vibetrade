"""
Прогон стратегии на исторических данных.

Использование:
    python -m src.backtest.runner
"""

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.detector import SetupDetector
from src.config import Settings
from src.storage.models import Candle

logger = logging.getLogger(__name__)

BACKTEST_DB = Path("data/backtest.db")
CYCLE_DELAY_BARS = 3  # проверять сетапы раз в N свечей (имитация цикла сканирования)


# ---------------------------------------------------------------------------
# Virtual position (без биржи)
# ---------------------------------------------------------------------------

class SimPosition:
    __slots__ = (
        "symbol", "entry_price", "entry_time", "quantity",
        "tp_price", "sl_price", "partial_closed", "partial_pnl",
        "closed", "exit_price", "exit_time", "pnl", "exit_reason",
    )

    def __init__(self, symbol: str, entry_price: float, entry_time: datetime,
                 quantity: float, tp_price: float, sl_price: float):
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


def _bar(rows, idx):
    """Безопасный доступ к списку свечей по индексу."""
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_backtest(config_path: str = "config/config.yaml") -> dict:
    """Запустить бэктест и вернуть статистику."""
    settings = Settings.from_yaml(config_path)

    engine = create_async_engine(f"sqlite+aiosqlite:///{BACKTEST_DB}", echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Загружаем все свечи, группируем по символу
    async with session_factory() as session:
        stmt = (
            select(Candle.symbol, Candle.timestamp, Candle.open, Candle.high,
                   Candle.low, Candle.close, Candle.volume)
            .where(Candle.exchange == "binance")
            .order_by(Candle.symbol, Candle.timestamp)
        )
        result = await session.execute(stmt)
        rows = result.all()

    # Группировка по символам
    symbols: dict[str, list] = {}
    for r in rows:
        symbols.setdefault(r[0], []).append(r)

    logger.info(f"Загружено символов: {len(symbols)}")

    # Ищем общий временной диапазон
    all_timestamps = sorted(set(r[1] for r in rows))
    if not all_timestamps:
        logger.error("Нет данных для бэктеста")
        return {}

    start_time = all_timestamps[0]
    end_time = all_timestamps[-1]
    logger.info(f"Период: {start_time} → {end_time}")
    logger.info(f"Всего временных срезов: {len(all_timestamps)}")

    # Детектор (без боевой БД — только данные в памяти)
    detector = SetupDetector(settings.strategy, timeframe=settings.collectors.timeframe)
    cfg = settings.trading

    positions: list[SimPosition] = []
    closed_trades: list[SimPosition] = []
    signals_count = 0

    # Проходим по каждому временному срезу (свече)
    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < detector.config.baseline_bars + detector.config.sustain_bars:
            continue  # недостаточно истории для детектора

        # Проверяем открытые позиции на TP/SL/время
        for pos in list(positions):
            if pos.closed:
                continue

            sym_data = symbols.get(pos.symbol)
            if not sym_data:
                continue

            # Ищем свечу с текущим таймстемпом для этой монеты
            current_bar = None
            bar_idx = -1
            for i, r in enumerate(sym_data):
                if r[1] == ts:
                    current_bar = r
                    bar_idx = i
                    break

            if not current_bar:
                continue

            high = current_bar[3]
            low = current_bar[4]
            close = current_bar[5]

            # Перевод стопа в б/у на полпути (без частичной фиксации)
            if cfg.breakeven_at_halfway and not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    cfg.partial_close_pct / 100
                )
                if high >= trigger:
                    pos.partial_closed = True
                    pos.sl_price = pos.entry_price
                    continue

            # Частичное закрытие (+ стоп в б/у)
            if cfg.partial_close_enabled and not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    cfg.partial_close_pct / 100
                )
                if high >= trigger:
                    close_qty = pos.quantity / 2
                    partial_pnl = (trigger - pos.entry_price) * close_qty
                    pos.quantity -= close_qty
                    pos.partial_closed = True
                    pos.partial_pnl = partial_pnl
                    pos.sl_price = pos.entry_price
                    continue

            # TP
            if high >= pos.tp_price:
                pos.exit_price = pos.tp_price
                pos.exit_time = ts
                pos.exit_reason = "tp"
                pos.pnl = (pos.tp_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                continue

            # SL
            if low <= pos.sl_price:
                pos.exit_price = pos.sl_price
                pos.exit_time = ts
                pos.exit_reason = "sl"
                pos.pnl = (pos.sl_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                continue

            # Выход по времени
            age = (ts - pos.entry_time).total_seconds() / 3600
            if age >= cfg.max_hold_hours:
                pos.exit_price = close
                pos.exit_time = ts
                pos.exit_reason = "time"
                pos.pnl = (close - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)

        # Ищем сетапы только в «циклы сканирования» (имитация задержки)
        if ts_idx % CYCLE_DELAY_BARS != 0:
            continue
        if len(positions) >= cfg.max_positions:
            continue

        for sym, sym_data in symbols.items():
            if len(positions) >= cfg.max_positions:
                break

            # Ищем индекс текущей свечи для этого символа
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

            # Проверяем кулдаун и дубликаты
            base = sym.split("/")[0].upper()
            if base in getattr(detector, '_exclude_coins', set()):
                continue
            if any(p.symbol == sym for p in positions):
                continue

            cooldown_cutoff = ts - timedelta(hours=24)
            if any(
                t.symbol == sym and t.exit_time and t.exit_time >= cooldown_cutoff
                for t in closed_trades
            ):
                continue

            # Собираем свечи до текущего момента
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

            # Детектор (только volume + price, без OI)
            if not detector._check_volume_pattern(candle_slice):
                continue
            direction = detector._check_price_trend(candle_slice)
            if direction != "long":
                continue
            # OI пропускаем — нет исторических данных

            signals_count += 1

            # Открываем позицию
            entry_price = candle_slice[-1]["close"]
            tp = entry_price * (1 + cfg.take_profit_pct / 100)
            sl = entry_price * (1 - cfg.stop_loss_pct / 100)
            qty = cfg.position_size_usdt / entry_price

            pos = SimPosition(
                symbol=sym,
                entry_price=entry_price,
                entry_time=ts,
                quantity=qty,
                tp_price=tp,
                sl_price=sl,
            )
            positions.append(pos)

    # Закрываем оставшиеся позиции по последней цене
    for pos in positions:
        sym_data = symbols.get(pos.symbol)
        if sym_data:
            last_close = sym_data[-1][5]
            pos.exit_price = last_close
            pos.exit_time = end_time
            pos.exit_reason = "eod"
            pos.pnl = (last_close - pos.entry_price) * pos.quantity + pos.partial_pnl
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

    await engine.dispose()

    return {
        "signals": signals_count,
        "trades": len(closed_trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(closed_trades), 2) if closed_trades else 0,
        "tp_wins": tp_wins,
        "sl_losses": sl_losses,
        "time_exits": time_exits,
        "partials": partials,
        "best": max(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "worst": min(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "period": f"{start_time} → {end_time}",
        "trades_list": closed_trades,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Бэктест торговой стратегии")
    parser.add_argument(
        "--config", type=str, default="config/config.yaml",
        help="Путь к конфигу (можно указать отдельный конфиг для тестов)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    result = asyncio.run(run_backtest(args.config))

    if not result:
        print("Нет данных")
        return

    print("\n" + "=" * 60)
    print("  РЕЗУЛЬТАТЫ БЭКТЕСТА")
    print("=" * 60)
    print(f"  Период:            {result['period']}")
    print(f"  Сигналов:          {result['signals']}")
    print(f"  Сделок:            {result['trades']}")
    print(f"  Плюс / Минус:      {result['wins']} / {result['losses']}")
    print(f"  Win rate:          {result['win_rate']}%")
    print(f"  Total PnL:         ${result['total_pnl']:+.2f}")
    print(f"  Средний PnL:       ${result['avg_pnl']:+.2f}")
    print(f"  TP: {result['tp_wins']} | SL: {result['sl_losses']} | Time: {result['time_exits']}")
    print(f"  Частичных закрытий: {result['partials']}")
    if result["best"]:
        print(f"  Лучшая:  {result['best'].symbol} ${result['best'].pnl:+.2f}")
    if result["worst"]:
        print(f"  Худшая:  {result['worst'].symbol} ${result['worst'].pnl:+.2f}")
    print("=" * 60)

    # Детализация по сделкам
    print("\n  Последние 20 сделок:")
    for t in result["trades_list"][-20:]:
        emoji = "✅" if t.exit_reason == "tp" else ("🛑" if t.exit_reason == "sl" else "⏰")
        pnl_pct = (t.exit_price / t.entry_price - 1) * 100
        print(
            f"  {emoji} {t.symbol:25s} "
            f"вход=${t.entry_price:.6f} выход=${t.exit_price:.6f} "
            f"PnL=${t.pnl:+.2f} ({pnl_pct:+.1f}%)  [{t.exit_reason}]"
        )


if __name__ == "__main__":
    main()
