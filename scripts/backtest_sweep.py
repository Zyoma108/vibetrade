"""
Batch backtest runner — sweeps key parameters to find optimal config.

Usage:
    python scripts/backtest_sweep.py
"""

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on path (for direct invocation)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Settings

DB_PATHS = [
    "data/trading_bot_22.06-30-06.db",
    "data/trading_bot.db",
]
CONFIG_PATH = "config/config.yaml"
HAS_OI = True  # DB has OI data


def db_name(db_path: str) -> str:
    """Short name for a DB path."""
    return Path(db_path).stem.replace("trading_bot_", "")


def make_config(overrides: dict) -> Settings:
    """Load base config and apply overrides."""
    s = Settings.from_yaml(CONFIG_PATH)
    for key, value in overrides.items():
        if hasattr(s.strategy, key):
            setattr(s.strategy, key, value)
        elif hasattr(s.trading, key):
            setattr(s.trading, key, value)
    return s


async def run_one(label: str, overrides: dict, db_path: str) -> dict | None:
    """Run a single backtest with given overrides on a specific DB."""
    cfg = make_config(overrides)
    start = time.time()
    try:
        result = await run_backtest_with_config(cfg, db_path, HAS_OI)
        elapsed = time.time() - start
        if result and result["trades"] > 0:
            # Combined score: balances Win Rate and PnL
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
            }
    except Exception as e:
        print(f"  ERROR [{label}]: {e}")
    return None


async def run_backtest_with_config(cfg: Settings, db_path: str, has_oi: bool) -> dict:
    """Run backtest with a Settings object instead of config path."""
    import numpy as np
    from datetime import datetime, timedelta, timezone
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
    except Exception:
        pass  # Старая БД без market_context_snapshots — фильтрация отключена

    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < detector.config.baseline_bars + detector.config.sustain_bars:
            continue

        # Определяем режим рынка на этот момент времени
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

        # Применяем множитель рыночного режима к детектору
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

            # TP/SL: фиксированные проценты от цены входа
            sl_distance = entry_price * (trading_cfg.stop_loss_pct / 100)
            tp_distance = sl_distance * trading_cfg.risk_reward_ratio

            # Бюджет риска: % от виртуального депозита $1000
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


def print_table(results: list[dict], title: str):
    """Pretty-print a comparison table."""
    print()
    print(f"  {title}")
    print(f"  {'─' * 80}")
    header = f"  {'Label':<28s} {'Trd':>4s} {'Win%':>6s} {'PnL':>8s} {'TP':>4s} {'SL':>4s} {'Tm':>4s} {'Part':>5s} {'Sig':>4s} {'PF':>5s}"
    print(header)
    print(f"  {'─' * 80}")
    for r in results:
        if r is None:
            continue
        # Calculate profit factor
        gross_profit = r["total_pnl"]
        pnl_str = f"${r['total_pnl']:+.2f}" if r["total_pnl"] != 0 else "$0.00"
        print(
            f"  {r['label']:<28s} {r['trades']:>4d} {r['win_rate']:>5.1f}% "
            f"{pnl_str:>8s} {r['tp']:>4d} {r['sl']:>4d} "
            f"{r['time_exits']:>4d} {r['partials']:>5d} {r['signals']:>4d}"
        )
    print()


async def run_round(label: str, overrides_list: list[tuple[str, dict]], db_path: str) -> list[dict]:
    """Run a round of backtests: iterate over parameter combos on one DB."""
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

    # RR and SL values for combined sweep
    RR_VALUES = [2.0, 2.5, 3.0, 3.5, 4.0]
    SL_VALUES = [3.0, 4.0, 5.0, 6.0, 7.5, 8.0, 10.0]

    for db_path in DB_PATHS:
        dname = db_name(db_path)
        if not Path(db_path).exists():
            print(f"\n  ⚠️  DB not found: {db_path} — skipping")
            continue

        print("\n" + "=" * 80)
        print(f"  BACKTEST SWEEP — {db_path} ({dname})")
        print("=" * 80)

        all_results = []

        # =====================================================================
        # BASELINE
        # =====================================================================
        print("\n[1/3] Baseline (current config)...")
        baseline = await run_one(f"BASELINE", {}, db_path)
        if baseline:
            all_results.append(baseline)
            print(f"  → {baseline['trades']}trd, {baseline['win_rate']}%WR, "
                  f"PnL=${baseline['total_pnl']:+.2f}, score={baseline['score']:.1f}")

        # =====================================================================
        # ROUND 1: Combined RR × SL sweep
        # =====================================================================
        print(f"\n[2/3] Combined RR×SL sweep ({len(RR_VALUES)}×{len(SL_VALUES)}={len(RR_VALUES)*len(SL_VALUES)} combos)...")
        combos = []
        for rr in RR_VALUES:
            for sl in SL_VALUES:
                combos.append((f"RR={rr:.1f}_SL={sl:.1f}%", {
                    "risk_reward_ratio": rr,
                    "stop_loss_pct": sl,
                }))
        combo_results = await run_round("", combos, db_path)
        all_results.extend(combo_results)

        # =====================================================================
        # ROUND 2: Other individual parameter sweeps
        # =====================================================================
        print("\n[3/3] Individual parameter sweeps...")

        # volume_surge_mult
        vol_combos = [(f"vol=x{vol:.0f}", {"volume_surge_mult": vol})
                      for vol in [10.0, 12.0, 15.0, 18.0, 20.0, 25.0]]
        vol_results = await run_round("", vol_combos, db_path)
        all_results.extend(vol_results)

        # cooldown_hours
        cool_combos = [(f"cool={ch}h", {"cooldown_hours": ch})
                       for ch in [0.0, 0.5, 1.0, 2.0, 4.0, 8.0]]
        cool_results = await run_round("", cool_combos, db_path)
        all_results.extend(cool_results)

        # sustain_bars
        sus_combos = [(f"sustain={sb}", {"sustain_bars": sb})
                      for sb in [3, 4, 5, 6]]
        sus_results = await run_round("", sus_combos, db_path)
        all_results.extend(sus_results)

        # exhaustion_gain_pct
        exh_combos = [(f"exhaust={eg:.0f}%", {"exhaustion_gain_pct": eg})
                      for eg in [0.0, 5.0, 8.0, 10.0]]
        exh_results = await run_round("", exh_combos, db_path)
        all_results.extend(exh_results)

        # dump_volume_mult
        dump_combos = [(f"dump={dv:.0f}", {"dump_volume_mult": dv})
                       for dv in [0.0, 2.0, 3.0, 5.0, 8.0]]
        dump_results = await run_round("", dump_combos, db_path)
        all_results.extend(dump_results)

        # partial_close_pct
        pc_combos = [(f"partial={pc:.0f}%", {"partial_close_pct": pc})
                     for pc in [40.0, 50.0, 60.0]]
        pc_results = await run_round("", pc_combos, db_path)
        all_results.extend(pc_results)

        # =====================================================================
        # SUMMARY
        # =====================================================================
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"  RESULTS — {dname} ({len(all_results)} runs)")
        print("=" * 80)

        # Print combined RR×SL table
        print(f"\n  COMBINED RR×SL SWEEP (score = PnL × WR/100)")
        print(f"  {'─' * 90}")
        header = f"  {'RR\\SL':>6s}"
        for sl in SL_VALUES:
            header += f" {f'SL={sl:.1f}%':>13s}"
        print(header)
        print(f"  {'─' * 90}")
        for rr in RR_VALUES:
            row = f"  {f'RR={rr:.1f}':>6s}"
            for sl in SL_VALUES:
                match = [r for r in combo_results
                         if f"RR={rr:.1f}_SL={sl:.1f}%" in r.get("label", "")]
                if match:
                    r = match[0]
                    row += f" {r['win_rate']:>4.0f}%${r['total_pnl']:>+7.1f}"
                else:
                    row += f" {'—':>13s}"
            print(row)
        print(f"  {'─' * 90}")

        # Top-5 by combined score
        print(f"\n  🏆 TOP-5 BY COMBINED SCORE (WR×PnL)")
        print(f"  {'─' * 70}")
        print(f"  {'Rank':<5s} {'Config':<30s} {'Trd':>4s} {'WR':>6s} {'PnL':>9s} {'Score':>8s}")
        print(f"  {'─' * 70}")
        sorted_by_score = sorted(all_results, key=lambda r: r["score"], reverse=True)
        for i, r in enumerate(sorted_by_score[:5]):
            pnl_str = f"${r['total_pnl']:+.2f}"
            print(f"  {i+1:<5d} {r['label']:<30s} {r['trades']:>4d} "
                  f"{r['win_rate']:>5.1f}% {pnl_str:>9s} {r['score']:>8.1f}")

        # Best vs baseline
        if baseline:
            best = sorted_by_score[0]
            print(f"\n  📊 BASELINE vs BEST")
            print(f"  {'─' * 60}")
            print(f"  {'':<20s} {'Trades':>6s} {'WR':>7s} {'PnL':>9s} {'Score':>8s}")
            print(f"  {'─' * 60}")
            print(f"  {'BASELINE':<20s} {baseline['trades']:>6d} {baseline['win_rate']:>6.1f}% "
                  f"${baseline['total_pnl']:>+8.2f} {baseline['score']:>8.1f}")
            print(f"  {'BEST: '+best['label']:<20s} {best['trades']:>6d} {best['win_rate']:>6.1f}% "
                  f"${best['total_pnl']:>+8.2f} {best['score']:>8.1f}")

        # Save per-DB results
        out = {
            "timestamp": datetime.now().isoformat(),
            "db": db_path,
            "results": all_results,
            "best_label": best["label"],
            "best_score": best["score"],
        }
        out_path = Path(f"data/backtest_sweep_{dname}.json")
        out_path.write_text(json.dumps(out, indent=2, default=str))
        print(f"\n  Results saved to {out_path}")

    print(f"\n{'=' * 80}")
    print(f"  ALL DONE — {total_time/60:.0f} min total")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    asyncio.run(main())
