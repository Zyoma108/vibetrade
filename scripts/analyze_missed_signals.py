"""
Analyze missed trading opportunities across databases.

Tests key theories:
1. Absolute dollar volume filter (MIN_ABSOLUTE_DOLLAR_VOL)
2. Dynamic surge multiplier based on baseline_usdt
3. Combination of both

Usage:
    .venv/bin/python scripts/analyze_missed_signals.py [--db data/trading_bot.db]
"""

import argparse
import sqlite3
from collections import defaultdict

import numpy as np


# Detector config (mirrors config.yaml)
BASELINE_BARS = 70
SUSTAIN_BARS = 4
MIN_BASELINE_VOLUME_USDT = 3000
SMOOTH_MAX_RATIO = 5.0
PRICE_GROWTH_MIN_PCT = 1.0
PRICE_GROWTH_MAX_PCT = 12.0
EXHAUSTION_GAIN_PCT = 5.0
EXHAUSTION_POS_RATIO = 0.7
MAX_HOURLY_DROP_PCT = 10.0
HOUR_BARS = 20


def get_dynamic_mult(baseline_usdt, base_mult=15.0, ref=10000.0, floor=4.0, cap=25.0):
    """Dynamic surge threshold: lower for liquid coins, higher for dust.

    At $10K baseline → base_mult (15x). Scales inversely with sqrt.
    """
    if baseline_usdt <= 0:
        return cap
    adjusted = base_mult * (ref / baseline_usdt) ** 0.5
    return max(floor, min(cap, adjusted))


def simulate_trade(candles, entry_idx):
    """Simulate trade outcome with partial close at 40% of TP path.

    Config: SL -5%, TP +15% (3:1 risk/reward), partial close at 40%.
    After partial: SL moves to breakeven, 50% position closed.

    Returns PnL as percentage of full position size.
    """
    entry = candles[entry_idx]["close"]

    sl = entry * 0.95
    tp = entry * 1.15
    partial_trigger = entry + (tp - entry) * 0.4

    hit_tp = False
    hit_sl = False
    hit_partial = False
    partial_pnl = 0.0

    for j in range(entry_idx + 1, min(entry_idx + 500, len(candles))):
        c = candles[j]

        # Partial close check
        if not hit_partial and c["high"] >= partial_trigger:
            hit_partial = True
            partial_pnl = (partial_trigger / entry - 1) * 0.5  # 50% of position at +6%

        # TP check
        if c["high"] >= tp:
            hit_tp = True
            if hit_partial:
                return partial_pnl + (tp / entry - 1) * 0.5
            else:
                return (tp / entry - 1)

        # SL check (breakeven after partial)
        effective_sl = entry if hit_partial else sl
        if c["low"] <= effective_sl:
            hit_sl = True
            if hit_partial:
                return partial_pnl + 0.0  # breakeven on remainder
            else:
                return (effective_sl / entry - 1)

    # Still open at end of data
    last_price = candles[-1]["close"]
    if hit_partial:
        return partial_pnl + (last_price / entry - 1) * 0.5
    else:
        return last_price / entry - 1


def analyze_database(db_path: str, min_abs_dollar_vol: float = 75000):
    """Full analysis of one database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ── Load data ──────────────────────────────────────────────
    symbols_candles = defaultdict(list)
    cursor = conn.execute(
        "SELECT symbol, timestamp, open, high, low, close, volume "
        "FROM candles WHERE volume > 0 ORDER BY symbol, timestamp"
    )
    for row in cursor:
        symbols_candles[row["symbol"]].append(dict(row))

    # ── Collect all passing windows for each threshold type ────
    thresholds_to_test = {
        "fixed_15x": lambda bu, bv, avg_r, avg_rusdt: 15.0,
        "fixed_10x": lambda bu, bv, avg_r, avg_rusdt: 10.0,
        "fixed_8x": lambda bu, bv, avg_r, avg_rusdt: 8.0,
        "fixed_6x": lambda bu, bv, avg_r, avg_rusdt: 6.0,
        "dynamic_sqrt": lambda bu, bv, avg_r, avg_rusdt: get_dynamic_mult(bu),
    }

    all_results = {name: [] for name in thresholds_to_test}

    for symbol, candles in symbols_candles.items():
        if len(candles) < BASELINE_BARS + SUSTAIN_BARS:
            continue

        for i in range(BASELINE_BARS + SUSTAIN_BARS - 1, len(candles)):
            window = candles[i - (BASELINE_BARS + SUSTAIN_BARS) + 1 : i + 1]

            volumes = np.array([c["volume"] for c in window])
            closes = np.array([c["close"] for c in window])
            opens_s = np.array([c["open"] for c in window[-SUSTAIN_BARS:]])

            baseline_vol = np.median(volumes[:BASELINE_BARS])
            if baseline_vol <= 0:
                continue

            baseline_closes = np.array([c["close"] for c in window[:BASELINE_BARS]])
            median_price = np.median(baseline_closes)
            baseline_usdt = baseline_vol * median_price

            if baseline_usdt < MIN_BASELINE_VOLUME_USDT:
                continue

            recent = volumes[-SUSTAIN_BARS:]
            avg_recent = np.mean(recent)
            avg_recent_price = np.mean([c["close"] for c in window[-SUSTAIN_BARS:]])
            avg_recent_usdt = avg_recent * avg_recent_price
            surge_mult = float(avg_recent / baseline_vol)

            # ── Quick pre-filters (same for all thresholds) ─────
            # Smoothness
            recent_median = np.median(recent)
            if recent_median > 0 and np.max(recent) / recent_median > SMOOTH_MAX_RATIO:
                continue

            # Price: valid open
            if opens_s[0] <= 0:
                continue

            change_pct = (closes[-1] / opens_s[0] - 1) * 100

            # Price: growth range
            if change_pct < PRICE_GROWTH_MIN_PCT or change_pct > PRICE_GROWTH_MAX_PCT:
                continue

            # Price: exhaustion
            if EXHAUSTION_GAIN_PCT > 0 and change_pct > EXHAUSTION_GAIN_PCT:
                last_c = window[-1]
                candle_range = last_c["high"] - last_c["low"]
                if candle_range > 0:
                    close_pos = (last_c["close"] - last_c["low"]) / candle_range
                    if close_pos > EXHAUSTION_POS_RATIO:
                        continue

            # Price: hourly drop
            if MAX_HOURLY_DROP_PCT > 0 and len(closes) >= HOUR_BARS:
                recent_low = np.min(closes[-HOUR_BARS:])
                ref_price = closes[-HOUR_BARS]
                if ref_price > 0:
                    drop = (recent_low / ref_price - 1) * 100
                    if drop <= -MAX_HOURLY_DROP_PCT:
                        continue

            # ── Test each threshold ─────────────────────────────
            for name, thresh_fn in thresholds_to_test.items():
                threshold = thresh_fn(baseline_usdt, baseline_vol, avg_recent, avg_recent_usdt)

                if surge_mult < threshold:
                    continue

                # Absolute dollar volume filter
                if avg_recent_usdt < min_abs_dollar_vol:
                    continue

                pnl = simulate_trade(candles, i)
                all_results[name].append({
                    "symbol": symbol,
                    "baseline_usdt": baseline_usdt,
                    "threshold": threshold,
                    "surge_mult": surge_mult,
                    "avg_recent_usdt": avg_recent_usdt,
                    "change_pct": change_pct,
                    "pnl_pct": pnl * 100,
                })

    conn.close()
    return all_results


def print_results(results: dict, label: str, min_abs_dollar_vol: float):
    """Print formatted results for one database."""
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"  Absolute dollar vol filter: sustain avg ≥ ${min_abs_dollar_vol:,.0f}")
    print(f"{'='*80}")
    print(f"{'Threshold':<16} {'Signals':>8} {'Wins':>6} {'WinRate':>8} {'AvgPnL':>8} {'TotalPnL':>10} {'Expectancy':>10}")
    print("-" * 75)

    for name in ["fixed_15x", "fixed_10x", "fixed_8x", "fixed_6x", "dynamic_sqrt"]:
        signals = results[name]
        total = len(signals)
        if total == 0:
            continue
        wins = sum(1 for s in signals if s["pnl_pct"] > 0)
        win_rate = wins / total * 100
        avg_pnl = np.mean([s["pnl_pct"] for s in signals])
        total_pnl = sum(s["pnl_pct"] for s in signals)
        # Expectancy per trade with 3:1 risk/reward
        exp_per_trade = win_rate / 100 * 15.0 - (1 - win_rate / 100) * 5.0

        print(f"{name:<16} {total:>8} {wins:>6} {win_rate:>7.1f}% {avg_pnl:>+7.1f}% {total_pnl:>+9.1f}% {exp_per_trade:>+9.1f}%")

    # BLEND-specific check
    print(f"\n  --- BLEND check ---")
    for name in ["fixed_15x", "fixed_10x", "fixed_8x", "fixed_6x", "dynamic_sqrt"]:
        blend_signals = [s for s in results[name] if "BLEND" in s["symbol"]]
        if blend_signals:
            for bs in blend_signals:
                print(f"  {name}: surge={bs['surge_mult']:.1f}x thresh={bs['threshold']:.1f}x "
                      f"base_usdt=${bs['baseline_usdt']:,.0f} avg_usdt=${bs['avg_recent_usdt']:,.0f} "
                      f"change={bs['change_pct']:+.1f}% pnl={bs['pnl_pct']:+.1f}%")
        else:
            print(f"  {name}: NO SIGNAL for BLEND")


def main():
    parser = argparse.ArgumentParser(description="Analyze missed trading signals")
    parser.add_argument("--db", default="data/trading_bot.db", help="Path to SQLite database")
    parser.add_argument("--all", action="store_true", help="Analyze all trading_bot_*.db files")
    parser.add_argument("--min-abs-vol", type=float, default=75000,
                        help="Minimum absolute dollar volume for sustain window (default: 75000)")
    args = parser.parse_args()

    if args.all:
        import glob
        db_files = sorted(glob.glob("data/trading_bot*.db"))
        print(f"Found {len(db_files)} databases")
    else:
        db_files = [args.db]

    for db_path in db_files:
        print(f"\nAnalyzing {db_path}...", flush=True)
        results = analyze_database(db_path, args.min_abs_vol)
        print_results(results, db_path, args.min_abs_vol)


if __name__ == "__main__":
    main()
