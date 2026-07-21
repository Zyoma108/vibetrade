"""
Focused backtest sweep — key parameters based on audit findings.
Tests RR×SL, volume surge, dump filter, partial close, risk %.

Usage:
    python scripts/sweep_focused.py
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Settings

# All three databases
DB_PATHS = [
    "data/trading_bot_22.06-30.06.db",
    "data/trading_bot_02.07-13.07.db",
    "data/trading_bot_13.07-20.07.db",
]
CONFIG_PATH = "config/config.yaml"
HAS_OI = True


def db_name(db_path: str) -> str:
    return Path(db_path).stem.replace("trading_bot_", "")


def make_config(overrides: dict) -> Settings:
    s = Settings.from_yaml(CONFIG_PATH)
    for key, value in overrides.items():
        if hasattr(s.strategy, key):
            setattr(s.strategy, key, value)
        elif hasattr(s.trading, key):
            setattr(s.trading, key, value)
    return s


async def run_one(label: str, overrides: dict, db_path: str) -> dict | None:
    cfg = make_config(overrides)
    start = time.time()
    try:
        result = await run_backtest_with_config(cfg, db_path, HAS_OI)
        elapsed = time.time() - start
        if result and result["trades"] > 0:
            score = result["total_pnl"] * (result["win_rate"] / 100)
            return {
                "label": label,
                "db": db_name(db_path),
                "trades": result["trades"],
                "wins": result["wins"],
                "losses": result["losses"],
                "win_rate": result["win_rate"],
                "total_pnl": result["total_pnl"],
                "avg_pnl": result["avg_pnl"],
                "tp": result["tp_wins"],
                "sl": result["sl_losses"],
                "time_exits": result["time_exits"],
                "partials": result["partials"],
                "signals": result["signals"],
                "elapsed": round(elapsed, 0),
                "score": round(score, 2),
                "overrides": overrides,
            }
    except Exception as e:
        print(f"  ERROR [{label}]: {e}")
    return None


# ---------------------------------------------------------------------------
# Inlined backtest engine (same as backtest_sweep.py)
# ---------------------------------------------------------------------------

class SimPosition:
    __slots__ = (
        "symbol", "entry_price", "entry_time", "quantity",
        "tp_price", "sl_price", "partial_closed", "partial_pnl",
        "closed", "exit_price", "exit_time", "pnl", "exit_reason",
    )

    def __init__(self, symbol, entry_price, entry_time, quantity,
                 tp_price=0.0, sl_price=0.0):
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
        self.exit_time = None
        self.pnl = 0.0
        self.exit_reason = ""


async def run_backtest_with_config(cfg: Settings, db_path: str, has_oi: bool) -> dict:
    import numpy as np
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from src.analytics.detector import SetupDetector
    from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct
    from src.storage.models import Candle, MarketContextSnapshot, OpenInterest

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

    positions: list[SimPosition] = []
    closed_trades: list[SimPosition] = []
    signals_count = 0

    oi_cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    if has_oi:
        async with session_factory() as session:
            oi_stmt = select(OpenInterest.exchange, OpenInterest.symbol,
                             OpenInterest.timestamp, OpenInterest.value).order_by(
                OpenInterest.exchange, OpenInterest.symbol, OpenInterest.timestamp)
            oi_result = await session.execute(oi_stmt)
            for ex, sym, ts, val in oi_result.all():
                oi_cache.setdefault((ex, sym), []).append((ts, val))

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
        "best": max(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "worst": min(closed_trades, key=lambda t: t.pnl) if closed_trades else None,
        "period": f"{all_timestamps[0]} → {all_timestamps[-1]}",
        "trades_list": closed_trades,
        "has_oi": has_oi,
        "has_mc": len(mc_snapshots) > 0,
    }


def _bar(rows, idx):
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


async def run_round(label_prefix: str, overrides_list: list[tuple[str, dict]], db_path: str) -> list[dict]:
    results = []
    for param_label, overrides in overrides_list:
        full_label = f"{param_label}"
        r = await run_one(full_label, overrides, db_path)
        if r:
            results.append(r)
            print(f"  → {param_label}: {r['trades']}trd, {r['win_rate']}%WR, "
                  f"PnL=${r['total_pnl']:+.2f}, score={r['score']:.1f}")
    return results


async def main():
    start_time = time.time()

    # =========================================================================
    # Focused parameter values
    # =========================================================================
    RR_VALUES = [2.0, 2.5, 3.0, 3.5]
    SL_VALUES = [3.0, 5.0, 7.0]          # 3% (old), 5% (proposed), 7% (wide)
    VOL_VALUES = [5.0, 6.5, 8.0]         # baseline, moderate, aggressive
    DUMP_VALUES = [0.0, 3.0]             # off, on
    RISK_VALUES = [0.6, 0.8, 1.0]        # low, medium, baseline
    PARTIAL_VALUES = [35.0, 40.0, 50.0]  # earlier, baseline, later
    COOLDOWN_VALUES = [0.5, 1.0, 2.0]    # short, baseline, long

    all_db_results = {}

    for db_path in DB_PATHS:
        dname = db_name(db_path)
        if not Path(db_path).exists():
            print(f"\n  ⚠️  DB not found: {db_path} — skipping")
            continue

        print("\n" + "=" * 80)
        print(f"  FOCUSED SWEEP — {db_path} ({dname})")
        print("=" * 80)

        all_results = []

        # =====================================================================
        # BASELINE (current config.yaml values)
        # =====================================================================
        print("\n[1/6] BASELINE (current config)...")
        baseline = await run_one("BASELINE", {}, db_path)
        if baseline:
            all_results.append(baseline)
            print(f"  → {baseline['trades']}trd, {baseline['win_rate']}%WR, "
                  f"PnL=${baseline['total_pnl']:+.2f}, score={baseline['score']:.1f}")

        # =====================================================================
        # ROUND 1: RR × SL — the most critical sweep
        # =====================================================================
        n_combos = len(RR_VALUES) * len(SL_VALUES)
        print(f"\n[2/6] RR×SL sweep ({len(RR_VALUES)}×{len(SL_VALUES)}={n_combos} combos)...")
        rr_sl_combos = []
        for rr in RR_VALUES:
            for sl in SL_VALUES:
                rr_sl_combos.append((f"RR={rr:.1f}_SL={sl:.1f}%", {
                    "risk_reward_ratio": rr,
                    "stop_loss_pct": sl,
                }))
        rr_sl_results = await run_round("", rr_sl_combos, db_path)
        all_results.extend(rr_sl_results)

        # =====================================================================
        # ROUND 2: Volume surge multiplier
        # =====================================================================
        print(f"\n[3/6] Volume surge sweep ({len(VOL_VALUES)} values)...")
        vol_combos = [(f"vol=x{vol:.1f}", {"volume_surge_mult": vol}) for vol in VOL_VALUES]
        vol_results = await run_round("", vol_combos, db_path)
        all_results.extend(vol_results)

        # =====================================================================
        # ROUND 3: Dump volume filter
        # =====================================================================
        print(f"\n[4/6] Dump volume filter sweep ({len(DUMP_VALUES)} values)...")
        dump_combos = [(f"dump={dv:.0f}", {"dump_volume_mult": dv}) for dv in DUMP_VALUES]
        dump_results = await run_round("", dump_combos, db_path)
        all_results.extend(dump_results)

        # =====================================================================
        # ROUND 4: Risk per trade % × Partial close %
        # =====================================================================
        n_rp = len(RISK_VALUES) * len(PARTIAL_VALUES)
        print(f"\n[5/6] Risk% × Partial% sweep ({len(RISK_VALUES)}×{len(PARTIAL_VALUES)}={n_rp} combos)...")
        rp_combos = []
        for risk in RISK_VALUES:
            for pc in PARTIAL_VALUES:
                rp_combos.append((f"risk={risk:.1f}%_pc={pc:.0f}%", {
                    "risk_per_trade_pct": risk,
                    "partial_close_pct": pc,
                }))
        rp_results = await run_round("", rp_combos, db_path)
        all_results.extend(rp_results)

        # =====================================================================
        # ROUND 5: Cooldown hours
        # =====================================================================
        print(f"\n[6/6] Cooldown sweep ({len(COOLDOWN_VALUES)} values)...")
        cool_combos = [(f"cool={ch}h", {"cooldown_hours": ch}) for ch in COOLDOWN_VALUES]
        cool_results = await run_round("", cool_combos, db_path)
        all_results.extend(cool_results)

        # =====================================================================
        # SUMMARY
        # =====================================================================
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"  RESULTS — {dname} ({len(all_results)} runs, {total_time/60:.1f} min)")
        print("=" * 80)

        # RR×SL table
        print(f"\n  RR×SL SWEEP (score = PnL × WR/100)")
        print(f"  {'─' * 75}")
        header = f"  {'RR\\SL':>6s}"
        for sl in SL_VALUES:
            header += f" {f'SL={sl:.1f}%':>14s}"
        print(header)
        print(f"  {'─' * 75}")
        for rr in RR_VALUES:
            row = f"  {f'RR={rr:.1f}':>6s}"
            for sl in SL_VALUES:
                match = [r for r in rr_sl_results
                         if f"RR={rr:.1f}_SL={sl:.1f}%" in r.get("label", "")]
                if match:
                    r = match[0]
                    row += f" {r['win_rate']:>4.0f}%${r['total_pnl']:>+7.1f}"
                else:
                    row += f" {'—':>14s}"
            print(row)
        print(f"  {'─' * 75}")

        # Top-10 by score
        print(f"\n  🏆 TOP-10 BY SCORE (WR×PnL)")
        print(f"  {'─' * 70}")
        print(f"  {'Rank':<5s} {'Config':<28s} {'Trd':>4s} {'WR':>6s} {'PnL':>9s} {'Score':>8s}")
        print(f"  {'─' * 70}")
        sorted_by_score = sorted(all_results, key=lambda r: r["score"], reverse=True)
        for i, r in enumerate(sorted_by_score[:10]):
            pnl_str = f"${r['total_pnl']:+.2f}"
            print(f"  {i+1:<5d} {r['label']:<28s} {r['trades']:>4d} "
                  f"{r['win_rate']:>5.1f}% {pnl_str:>9s} {r['score']:>8.1f}")

        # Best vs baseline
        if baseline and sorted_by_score:
            best = sorted_by_score[0]
            print(f"\n  📊 BASELINE vs BEST")
            print(f"  {'─' * 60}")
            print(f"  {'':<20s} {'Trades':>6s} {'WR':>7s} {'PnL':>9s} {'Score':>8s}")
            print(f"  {'─' * 60}")
            print(f"  {'BASELINE':<20s} {baseline['trades']:>6d} {baseline['win_rate']:>6.1f}% "
                  f"${baseline['total_pnl']:>+8.2f} {baseline['score']:>8.1f}")
            print(f"  {'BEST: '+best['label']:<20s} {best['trades']:>6d} {best['win_rate']:>6.1f}% "
                  f"${best['total_pnl']:>+8.2f} {best['score']:>8.1f}")

        all_db_results[dname] = {
            "all": all_results,
            "baseline": baseline,
            "best": best if sorted_by_score else None,
            "top10": sorted_by_score[:10],
        }

        # Save per-DB results
        out = {
            "timestamp": datetime.now().isoformat(),
            "db": db_path,
            "results": all_results,
            "best_label": best["label"] if sorted_by_score else "N/A",
            "best_score": best["score"] if sorted_by_score else 0,
        }
        out_path = Path(f"data/sweep_focused_{dname}.json")
        out_path.write_text(json.dumps(out, indent=2, default=str))
        print(f"\n  💾 Saved to {out_path}")

    # =========================================================================
    # CROSS-DB SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print("  CROSS-DB SUMMARY")
    print("=" * 80)

    print(f"\n  {'DB':<22s} {'Best Config':<28s} {'Trd':>4s} {'WR':>6s} {'PnL':>9s} {'Score':>8s}")
    print(f"  {'─' * 80}")
    for dname, data in all_db_results.items():
        if data["baseline"]:
            b = data["baseline"]
            print(f"  {dname+' BASELINE':<22s} {'(current config)':<28s} "
                  f"{b['trades']:>4d} {b['win_rate']:>5.1f}% "
                  f"${b['total_pnl']:>+8.2f} {b['score']:>8.1f}")
        if data["best"]:
            r = data["best"]
            print(f"  {dname+' BEST':<22s} {r['label']:<28s} "
                  f"{r['trades']:>4d} {r['win_rate']:>5.1f}% "
                  f"${r['total_pnl']:>+8.2f} {r['score']:>8.1f}")
        print(f"  {'─' * 80}")

    print(f"\n{'=' * 80}")
    print(f"  ALL DONE — {total_time/60:.0f} min total")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    asyncio.run(main())
