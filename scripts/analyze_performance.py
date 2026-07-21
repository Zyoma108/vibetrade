"""
Comprehensive performance analysis across multiple databases.
Runs backtests with different parameters and compares results.

Usage:
    python scripts/analyze_performance.py
"""

import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.detector import SetupDetector
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct
from src.config import Settings
from src.storage.models import Candle, OpenInterest


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
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


async def run_backtest_with_config(
    db_path: str,
    config_overrides: dict,
    base_config_path: str = "config/config.yaml",
    has_oi: bool = True,
) -> dict:
    """Run backtest with given config overrides."""
    settings = Settings.from_yaml(base_config_path)
    for key, value in config_overrides.items():
        if hasattr(settings.strategy, key):
            setattr(settings.strategy, key, value)
        elif hasattr(settings.trading, key):
            setattr(settings.trading, key, value)

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
    detector = SetupDetector(settings.strategy, timeframe=settings.collectors.timeframe)
    trading_cfg = settings.trading

    positions: list[SimPosition] = []
    closed_trades: list[SimPosition] = []
    signals_count = 0
    rejected_volume = 0
    rejected_price = 0
    rejected_oi = 0

    oi_cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    if has_oi:
        async with session_factory() as session:
            oi_stmt = select(OpenInterest.exchange, OpenInterest.symbol,
                             OpenInterest.timestamp, OpenInterest.value).order_by(
                OpenInterest.exchange, OpenInterest.symbol, OpenInterest.timestamp)
            oi_result = await session.execute(oi_stmt)
            for ex, sym, ts, val in oi_result.all():
                oi_cache.setdefault((ex, sym), []).append((ts, val))

    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < detector.config.baseline_bars + detector.config.sustain_bars:
            continue

        # Check open positions
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

            high = current_bar[3]
            low = current_bar[4]
            close = current_bar[5]


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
                rejected_volume += 1
                continue

            direction = detector.check_price_trend(candle_slice)
            if direction != "long":
                rejected_price += 1
                continue

            if has_oi:
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
                    rejected_oi += 1
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

    # Calculate profit factor
    gross_profit = sum(t.pnl for t in closed_trades if t.pnl > 0)
    gross_loss = abs(sum(t.pnl for t in closed_trades if t.pnl < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

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
        "profit_factor": round(profit_factor, 2),
        "period": f"{all_timestamps[0]} → {all_timestamps[-1]}",
        "trades_list": closed_trades,
        "rejected_volume": rejected_volume,
        "rejected_price": rejected_price,
        "rejected_oi": rejected_oi,
    }


def print_result(label: str, r: dict, highlight: bool = False):
    """Print a single backtest result."""
    prefix = "🏆 " if highlight else "   "
    if not r:
        print(f"{prefix}{label}: NO TRADES")
        return
    pnl_str = f"${r['total_pnl']:+.2f}"
    print(
        f"{prefix}{label:<45s} "
        f"Trades={r['trades']:>3d}  WR={r['win_rate']:>5.1f}%  "
        f"PnL={pnl_str:>8s}  PF={r['profit_factor']:>5.2f}  "
        f"TP={r['tp_wins']:>2d}  SL={r['sl_losses']:>2d}  "
        f"Time={r['time_exits']:>2d}  Part={r['partials']:>2d}  "
        f"Sig={r['signals']:>3d}"
    )


async def main():
    DB_BAD = "data/trading_bot_07.06-16.06.db"
    DB_GOOD = "data/trading_bot_20.05-27.05.db"
    # DB_MID = "data/trading_bot_29.05-07.06.db"  # could also analyze

    start_time = time.time()

    # =========================================================================
    # 1. BASELINE on both DBs
    # =========================================================================
    print("=" * 110)
    print("  BASELINE (current config) on both databases")
    print("=" * 110)

    baseline_bad = await run_backtest_with_config(DB_BAD, {})
    baseline_good = await run_backtest_with_config(DB_GOOD, {})

    print_result("BAD  (07.06-16.06) BASELINE", baseline_bad, highlight=True)
    print_result("GOOD (20.05-27.05) BASELINE", baseline_good, highlight=True)

    if baseline_bad:
        print(f"\n  BAD period rejected: vol={baseline_bad['rejected_volume']}, "
              f"price={baseline_bad['rejected_price']}, oi={baseline_bad['rejected_oi']}")
    if baseline_good:
        print(f"  GOOD period rejected: vol={baseline_good['rejected_volume']}, "
              f"price={baseline_good['rejected_price']}, oi={baseline_good['rejected_oi']}")

    # =========================================================================
    # 2. Parameter sweep on BAD database
    # =========================================================================
    print("\n" + "=" * 110)
    print("  PARAMETER SWEEP on BAD database (07.06-16.06)")
    print("=" * 110)

    sweeps = []

    # 2a. volume_surge_mult
    print("\n  --- volume_surge_mult ---")
    for vol in [15.0, 18.0, 20.0, 25.0, 30.0]:
        r = await run_backtest_with_config(DB_BAD, {"volume_surge_mult": vol})
        print_result(f"vol_surge=x{vol:.0f}", r)
        if r:
            sweeps.append(("vol", vol, r))

    # 2b. sustain_bars
    print("\n  --- sustain_bars ---")
    for sb in [3, 4, 5, 6]:
        r = await run_backtest_with_config(DB_BAD, {"sustain_bars": sb})
        print_result(f"sustain_bars={sb}", r)
        if r:
            sweeps.append(("sus", sb, r))

    # 2c. exhaustion_gain_pct
    print("\n  --- exhaustion_gain_pct ---")
    for eg in [0.0, 3.0, 5.0, 8.0, 10.0]:
        r = await run_backtest_with_config(DB_BAD, {"exhaustion_gain_pct": eg})
        print_result(f"exhaust_gain={eg:.0f}%", r)
        if r:
            sweeps.append(("exh", eg, r))

    # 2d. price_growth_min_pct
    print("\n  --- price_growth_min_pct ---")
    for pg in [0.5, 1.0, 1.5, 2.0, 3.0]:
        r = await run_backtest_with_config(DB_BAD, {"price_growth_min_pct": pg})
        print_result(f"price_growth_min={pg:.1f}%", r)
        if r:
            sweeps.append(("pg", pg, r))

    # 2e. price_growth_max_pct
    print("\n  --- price_growth_max_pct ---")
    for pm in [5.0, 8.0, 12.0, 15.0, 0.0]:
        r = await run_backtest_with_config(DB_BAD, {"price_growth_max_pct": pm})
        print_result(f"price_growth_max={pm:.0f}%", r)
        if r:
            sweeps.append(("pm", pm, r))

    # 2f. stop_loss_pct
    print("\n  --- stop_loss_pct ---")
    for slp in [3.0, 4.0, 5.0, 6.0, 7.5]:
        r = await run_backtest_with_config(DB_BAD, {"stop_loss_pct": slp})
        print_result(f"stop_loss={slp:.1f}%", r)
        if r:
            sweeps.append(("sl", slp, r))

    # 2g. risk_reward_ratio
    print("\n  --- risk_reward_ratio ---")
    for rr in [2.0, 2.5, 3.0, 4.0, 5.0]:
        r = await run_backtest_with_config(DB_BAD, {"risk_reward_ratio": rr})
        print_result(f"risk_reward={rr:.1f}", r)
        if r:
            sweeps.append(("rr", rr, r))

    # 2h. oi_slope_min_pct
    print("\n  --- oi_slope_min_pct ---")
    for oi in [0.5, 1.0, 2.0, 3.0, 5.0]:
        r = await run_backtest_with_config(DB_BAD, {"oi_slope_min_pct": oi})
        print_result(f"oi_slope_min={oi:.1f}%", r)
        if r:
            sweeps.append(("oi", oi, r))

    # 2i. smooth_max_ratio
    print("\n  --- smooth_max_ratio ---")
    for sm in [3.0, 4.0, 5.0, 7.0]:
        r = await run_backtest_with_config(DB_BAD, {"smooth_max_ratio": sm})
        print_result(f"smooth_max_ratio={sm:.0f}", r)
        if r:
            sweeps.append(("sm", sm, r))

    # 2j. dump_volume_mult
    print("\n  --- dump_volume_mult ---")
    for dv in [2.0, 3.0, 5.0, 8.0]:
        r = await run_backtest_with_config(DB_BAD, {"dump_volume_mult": dv})
        print_result(f"dump_volume_mult={dv:.0f}", r)
        if r:
            sweeps.append(("dv", dv, r))

    # 2k. baseline_bars
    print("\n  --- baseline_bars ---")
    for bb in [50, 70, 100]:
        r = await run_backtest_with_config(DB_BAD, {"baseline_bars": bb})
        print_result(f"baseline_bars={bb}", r)
        if r:
            sweeps.append(("bb", bb, r))

    # 2l. partial_close_pct
    print("\n  --- partial_close_pct ---")
    for pc in [30.0, 40.0, 50.0, 60.0]:
        r = await run_backtest_with_config(DB_BAD, {"partial_close_pct": pc})
        print_result(f"partial_close={pc:.0f}%", r)
        if r:
            sweeps.append(("pc", pc, r))

    # =========================================================================
    # 3. Best config on BAD → test on GOOD
    # =========================================================================
    if sweeps:
        best = max(sweeps, key=lambda s: s[2]["total_pnl"])
        print("\n" + "=" * 110)
        print(f"  BEST on BAD: {best[0]}={best[1]} → PnL=${best[2]['total_pnl']:+.2f}, "
              f"WR={best[2]['win_rate']}%, PF={best[2]['profit_factor']}")
        print("=" * 110)

        # Also find best by profit_factor
        best_pf = max(sweeps, key=lambda s: s[2]["profit_factor"])
        print(f"  BEST by PF: {best_pf[0]}={best_pf[1]} → PnL=${best_pf[2]['total_pnl']:+.2f}, "
              f"WR={best_pf[2]['win_rate']}%, PF={best_pf[2]['profit_factor']}")

    # Find top 3
    top3 = sorted(sweeps, key=lambda s: s[2]["total_pnl"], reverse=True)[:5]
    print("\n  --- TOP 5 by PnL on BAD DB ---")
    for t in top3:
        print_result(f"{t[0]}={t[1]}", t[2])

    # =========================================================================
    # 4. Cross-validation: test best configs on GOOD database
    # =========================================================================
    print("\n" + "=" * 110)
    print("  CROSS-VALIDATION: Apply best BAD configs to GOOD database")
    print("=" * 110)

    for t in top3[:3]:
        param_name = t[0]
        param_value = t[1]
        param_map = {
            "vol": "volume_surge_mult", "sus": "sustain_bars",
            "exh": "exhaustion_gain_pct", "pg": "price_growth_min_pct",
            "pm": "price_growth_max_pct", "sl": "stop_loss_pct",
            "rr": "risk_reward_ratio", "oi": "oi_slope_min_pct",
            "sm": "smooth_max_ratio", "dv": "dump_volume_mult",
            "bb": "baseline_bars", "pc": "partial_close_pct",
        }
        key = param_map.get(param_name, param_name)
        r = await run_backtest_with_config(DB_GOOD, {key: param_value})
        print_result(f"GOOD with {key}={param_value}", r)

    # =========================================================================
    # 5. Combinatorial best
    # =========================================================================
    print("\n" + "=" * 110)
    print("  COMBINATORIAL TESTS on BAD database")
    print("=" * 110)

    combos = [
        # (label, overrides)
        ("Tighter vol + sustain", {"volume_surge_mult": 20.0, "sustain_bars": 5}),
        ("Tighter vol + exhaust", {"volume_surge_mult": 20.0, "exhaustion_gain_pct": 3.0}),
        ("Higher OI + tighter vol", {"oi_slope_min_pct": 3.0, "volume_surge_mult": 20.0}),
        ("Tight vol + low exhaust + high OI", {
            "volume_surge_mult": 20.0, "exhaustion_gain_pct": 3.0, "oi_slope_min_pct": 3.0
        }),
        ("Max tightness", {
            "volume_surge_mult": 25.0, "sustain_bars": 5,
            "exhaustion_gain_pct": 2.0, "oi_slope_min_pct": 5.0,
            "price_growth_min_pct": 1.0, "price_growth_max_pct": 8.0,
        }),
        ("Higher SL + tighter vol", {
            "volume_surge_mult": 20.0, "stop_loss_pct": 7.0,
            "exhaustion_gain_pct": 3.0,
        }),
    ]

    combo_results = []
    for label, overrides in combos:
        r = await run_backtest_with_config(DB_BAD, overrides)
        print_result(label, r)
        if r:
            combo_results.append((label, r, overrides))
            # Cross-validate on GOOD
            r2 = await run_backtest_with_config(DB_GOOD, overrides)
            print_result(f"  → GOOD: {label}", r2)

    # =========================================================================
    # 6. Summary
    # =========================================================================
    total_time = time.time() - start_time
    print("\n" + "=" * 110)
    print(f"  ANALYSIS COMPLETE ({total_time/60:.1f} min)")
    print("=" * 110)

    print(f"\n  BAD period (07.06-16.06): {baseline_bad['trades']} trades, "
          f"WR={baseline_bad['win_rate']}%, PnL=${baseline_bad['total_pnl']:+.2f}")
    print(f"  GOOD period (20.05-27.05): {baseline_good['trades']} trades, "
          f"WR={baseline_good['win_rate']}%, PnL=${baseline_good['total_pnl']:+.2f}")

    if combo_results:
        best_combo = max(combo_results, key=lambda c: c[1]["total_pnl"])
        print(f"\n  🏆 BEST COMBO on BAD: {best_combo[0]}")
        print(f"     PnL=${best_combo[1]['total_pnl']:+.2f}, WR={best_combo[1]['win_rate']}%, "
              f"PF={best_combo[1]['profit_factor']}, Trades={best_combo[1]['trades']}")
        print(f"     Overrides: {best_combo[2]}")


if __name__ == "__main__":
    asyncio.run(main())
