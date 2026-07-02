"""
Focused RR×SL combined sweep — runs on one DB, prints progress in real time.
Usage:
    PYTHONUNBUFFERED=1 .venv/bin/python scripts/sweep_rr_sl.py
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Settings

DB_PATH = "data/trading_bot_22.06-30-06.db"  # full sweep on big DB
CONFIG_PATH = "config/config.yaml"


def make_config(overrides: dict) -> Settings:
    s = Settings.from_yaml(CONFIG_PATH)
    for key, value in overrides.items():
        if hasattr(s.strategy, key):
            setattr(s.strategy, key, value)
        elif hasattr(s.trading, key):
            setattr(s.trading, key, value)
    return s


async def run_backtest_with_config(cfg: Settings, db_path: str) -> dict:
    """Clone of sweep script's backtest runner."""
    import numpy as np
    from datetime import timedelta, timezone
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from src.analytics.detector import SetupDetector
    from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct
    from src.storage.models import Candle, MarketContextSnapshot, OpenInterest
    from src.backtest.runner import SimPosition

    db_path = Path(db_path)
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

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
        await engine.dispose()
        return {}

    CYCLE_DELAY_BARS = 3
    detector = SetupDetector(cfg.strategy, timeframe=cfg.collectors.timeframe)
    trading_cfg = cfg.trading

    positions: list = []
    closed_trades: list = []
    signals_count = 0

    oi_cache: dict = {}
    async with session_factory() as session:
        oi_stmt = select(OpenInterest.exchange, OpenInterest.symbol,
                         OpenInterest.timestamp, OpenInterest.value).order_by(
            OpenInterest.exchange, OpenInterest.symbol, OpenInterest.timestamp)
        oi_result = await session.execute(oi_stmt)
        for ex, sym, ts, val in oi_result.all():
            oi_cache.setdefault((ex, sym), []).append((ts, val))

    mc_snapshots: list = []
    try:
        async with session_factory() as session:
            mc_stmt = (
                select(MarketContextSnapshot)
                .order_by(MarketContextSnapshot.timestamp)
            )
            mc_result = await session.execute(mc_stmt)
            for row in mc_result.scalars().all():
                mc_snapshots.append((row.timestamp, row))
    except Exception:
        pass

    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < detector.config.baseline_bars + detector.config.sustain_bars:
            continue

        regime = "unknown"
        for mc_ts, mc_row in reversed(mc_snapshots):
            if mc_ts <= ts:
                regime = mc_row.regime
                break
        if regime == "risk_off":
            continue

        for pos in list(positions):
            if pos.closed:
                continue

            sym_data = symbols.get(pos.symbol)
            if not sym_data:
                continue

            current_bar = None
            for i, r in enumerate(sym_data):
                if r[1] == ts:
                    current_bar = r
                    break
            if not current_bar:
                continue

            high, low, close = current_bar[3], current_bar[4], current_bar[5]

            if trading_cfg.breakeven_at_halfway and not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    trading_cfg.partial_close_pct / 100)
                if high >= trigger:
                    pos.partial_closed = True
                    pos.sl_price = pos.entry_price
                    continue

            if not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    trading_cfg.partial_close_pct / 100)
                if high >= trigger:
                    close_qty = pos.quantity / 2
                    partial_pnl = (trigger - pos.entry_price) * close_qty
                    pos.quantity -= close_qty
                    pos.partial_closed = True
                    pos.partial_pnl = partial_pnl
                    pos.sl_price = pos.entry_price
                    continue

            if high >= pos.tp_price:
                pos.exit_price = pos.tp_price
                pos.exit_time = ts
                pos.exit_reason = "tp"
                pos.pnl = (pos.tp_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                continue

            if low <= pos.sl_price:
                pos.exit_price = pos.sl_price
                pos.exit_time = ts
                pos.exit_reason = "sl"
                pos.pnl = (pos.sl_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                continue

            age = (ts - pos.entry_time).total_seconds() / 3600
            if age >= trading_cfg.max_hold_hours:
                pos.exit_price = close
                pos.exit_time = ts
                pos.exit_reason = "time"
                pos.pnl = (close - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)

        if ts_idx % CYCLE_DELAY_BARS != 0:
            continue
        if len(positions) >= trading_cfg.max_positions:
            continue

        if regime == "cautious":
            increase_pct = detector.config.cautious_volume_surge_mult_increase_pct
            detector.apply_regime_multiplier(1.0 + increase_pct / 100.0)
        else:
            detector.apply_regime_multiplier(1.0)

        for sym, sym_data in symbols.items():
            if len(positions) >= trading_cfg.max_positions:
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

            cooldown_cutoff = ts - timedelta(hours=trading_cfg.cooldown_hours)
            if trading_cfg.cooldown_hours > 0 and any(
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

            if not detector.check_volume_pattern(candle_slice):
                continue
            direction = detector.check_price_trend(candle_slice)
            if direction != "long":
                continue

            oi_pass = False
            for ex in ("bybit", "binance"):
                key = (ex, sym)
                if key not in oi_cache:
                    continue
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

            entry_price = candle_slice[-1]["close"]
            sl_distance = entry_price * (trading_cfg.stop_loss_pct / 100)
            tp_distance = sl_distance * trading_cfg.risk_reward_ratio

            virtual_balance = 1000.0
            risk_budget = virtual_balance * (trading_cfg.risk_per_trade_pct / 100)
            qty = risk_budget / sl_distance
            tp = entry_price + tp_distance
            sl = entry_price - sl_distance

            pos = SimPosition(
                symbol=sym, entry_price=entry_price, entry_time=ts,
                quantity=qty, tp_price=tp, sl_price=sl,
            )
            positions.append(pos)

    for pos in positions:
        sym_data = symbols.get(pos.symbol)
        if sym_data:
            last_close = sym_data[-1][5]
            pos.exit_price = last_close
            pos.exit_time = all_timestamps[-1]
            pos.exit_reason = "eod"
            pos.pnl = (last_close - pos.entry_price) * pos.quantity + pos.partial_pnl
        pos.closed = True
        closed_trades.append(pos)

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
        "trades_list": closed_trades,
        "has_oi": True,
        "has_mc": len(mc_snapshots) > 0,
    }


def _bar(rows, idx):
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


async def main():
    RR_VALUES = [2.0, 2.5, 3.0, 3.5, 4.0]
    SL_VALUES = [3.0, 4.0, 5.0, 6.0, 7.5, 8.0, 10.0]

    print(f"\n{'='*90}")
    print(f"  RR × SL COMBINED SWEEP — {DB_PATH}")
    print(f"  Grid: RR={RR_VALUES}, SL={SL_VALUES}")
    print(f"  Total: {len(RR_VALUES)}×{len(SL_VALUES)} = {len(RR_VALUES)*len(SL_VALUES)} runs")
    print(f"{'='*90}\n")

    results = []
    start_time = time.time()
    total = len(RR_VALUES) * len(SL_VALUES)
    n = 0

    for rr in RR_VALUES:
        for sl in SL_VALUES:
            n += 1
            label = f"RR={rr:.1f} SL={sl:.1f}%"
            overrides = {"risk_reward_ratio": rr, "stop_loss_pct": sl}
            cfg = make_config(overrides)

            t0 = time.time()
            result = await run_backtest_with_config(cfg, DB_PATH)
            elapsed = time.time() - t0

            if result and result["trades"] > 0:
                score = result["total_pnl"] * (result["win_rate"] / 100)
                row = {
                    "label": label, "rr": rr, "sl": sl,
                    "trades": result["trades"], "wins": result["wins"],
                    "losses": result["losses"], "win_rate": result["win_rate"],
                    "total_pnl": result["total_pnl"], "avg_pnl": result["avg_pnl"],
                    "tp": result["tp_wins"], "sl_count": result["sl_losses"],
                    "time_exits": result["time_exits"], "partials": result["partials"],
                    "signals": result["signals"], "elapsed": round(elapsed, 0),
                    "score": round(score, 2),
                }
                results.append(row)
                eta = (total - n) * (elapsed / 60)  # remaining minutes
                print(f"  [{n:2d}/{total}] {label:20s} → {result['trades']:2d}trd "
                      f"{result['win_rate']:5.1f}%WR "
                      f"PnL=${result['total_pnl']:+7.2f} "
                      f"score={score:+6.1f} "
                      f"({elapsed:.0f}s, ETA {eta:.0f}m)",
                      flush=True)
            else:
                print(f"  [{n:2d}/{total}] {label:20s} → no trades", flush=True)

    total_time = time.time() - start_time

    # Print results matrix
    print(f"\n\n{'='*90}")
    print(f"  RR × SL MATRIX (cell = WR% / $PnL)")
    print(f"{'='*90}")
    # Header
    header = f"  {'RR\\SL':>8s}"
    for sl in SL_VALUES:
        header += f"  {f'SL={sl:.1f}%':>15s}"
    print(header)
    print(f"  {'─'*90}")
    for rr in RR_VALUES:
        row = f"  {f'RR={rr:.1f}':>8s}"
        for sl in SL_VALUES:
            match = [r for r in results if r["rr"] == rr and r["sl"] == sl]
            if match:
                r = match[0]
                row += f"  {r['win_rate']:>4.0f}% ${r['total_pnl']:>+8.2f}"
            else:
                row += f"  {'—':>15s}"
        print(row)

    # Top-5 by score
    print(f"\n\n  🏆 TOP-5 BY COMBINED SCORE (Score = PnL × WR/100)")
    print(f"  {'─'*75}")
    print(f"  {'Rank':<5s} {'Config':<20s} {'Trd':>4s} {'WR':>6s} {'PnL':>9s} {'TP':>4s} {'SL':>4s} {'Score':>8s}")
    print(f"  {'─'*75}")
    sorted_by_score = sorted(results, key=lambda r: r["score"], reverse=True)
    for i, r in enumerate(sorted_by_score[:5]):
        pnl_str = f"${r['total_pnl']:+.2f}"
        print(f"  {i+1:<5d} {r['label']:<20s} {r['trades']:>4d} "
              f"{r['win_rate']:>5.1f}% {pnl_str:>9s} {r['tp']:>4d} {r['sl_count']:>4d} {r['score']:>8.1f}")

    # Best vs baseline (RR=3.0 SL=5.0)
    baseline = [r for r in results if r["rr"] == 3.0 and r["sl"] == 5.0]
    if baseline and sorted_by_score:
        b = baseline[0]
        best = sorted_by_score[0]
        print(f"\n  📊 BASELINE vs BEST")
        print(f"  {'─'*65}")
        print(f"  {'':<20s} {'Trd':>5s} {'WR':>7s} {'PnL':>9s} {'TP':>4s} {'SL':>4s} {'Score':>8s}")
        print(f"  {'─'*65}")
        print(f"  {'BASELINE (RR=3 SL=5)':<20s} {b['trades']:>5d} {b['win_rate']:>6.1f}% "
              f"${b['total_pnl']:>+8.2f} {b['tp']:>4d} {b['sl_count']:>4d} {b['score']:>8.1f}")
        print(f"  {'BEST: '+best['label']:<20s} {best['trades']:>5d} {best['win_rate']:>6.1f}% "
              f"${best['total_pnl']:>+8.2f} {best['tp']:>4d} {best['sl_count']:>4d} {best['score']:>8.1f}")
        delta_score = best['score'] - b['score']
        delta_pnl = best['total_pnl'] - b['total_pnl']
        print(f"  {'─'*65}")
        print(f"  Δ = score: {delta_score:+.1f}, PnL: ${delta_pnl:+.2f}")

    # Save
    out = {
        "timestamp": datetime.now().isoformat(),
        "db": DB_PATH,
        "results": results,
        "best": sorted_by_score[0]["label"] if sorted_by_score else None,
    }
    out_path = Path("data/backtest_sweep_rr_sl.json")
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\n  💾 Saved to {out_path}")
    print(f"  ⏱️  Total: {total_time/60:.0f} min\n")


if __name__ == "__main__":
    asyncio.run(main())
