"""
Test a new "blow-off top" filter for the detector.
Detects: massive volume spike in recent bars followed by price rejection (long upper wick or reversal).

Usage:
    python scripts/test_blowoff_filter.py
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


def check_blowoff_top(candles: list[dict], sustain_bars: int,
                      lookback_bars: int = 8,
                      blowoff_volume_mult: float = 5.0,
                      blowoff_price_reversal_pct: float = 3.0) -> bool:
    """
    Check for blow-off top pattern: massive volume spike followed by price rejection.

    Looks at bars BEFORE the sustain window to detect pump-and-dump patterns
    where the strategy would otherwise enter on the dead cat bounce.

    Returns True if a blow-off pattern is detected (signal should be BLOCKED).
    """
    if len(candles) < sustain_bars + lookback_bars:
        return False

    # Separate pre-sustain bars from sustain bars
    pre_bars = candles[-(sustain_bars + lookback_bars):-sustain_bars]
    sustain_bars_list = candles[-sustain_bars:]

    if len(pre_bars) < 3:
        return False

    # 1. Check for extreme volume spike in pre-sustain bars
    pre_volumes = np.array([c["volume"] for c in pre_bars])
    pre_median = np.median(pre_volumes)
    if pre_median <= 0:
        return False

    max_vol = np.max(pre_volumes)
    max_vol_idx = np.argmax(pre_volumes)

    # The volume spike must be significant relative to other pre-sustain bars
    if max_vol / pre_median < blowoff_volume_mult:
        return False

    # 2. Check price reversal after the volume spike
    # The spike candle should have:
    # a) A significant upper wick (high - max(open, close)) / (high - low) > 50%
    # b) OR price dropped significantly after the spike
    spike_candle = pre_bars[max_vol_idx]
    spike_range = spike_candle["high"] - spike_candle["low"]
    if spike_range > 0:
        upper_wick = spike_candle["high"] - max(spike_candle["open"], spike_candle["close"])
        wick_ratio = upper_wick / spike_range

        # Long upper wick (>60% of range) = rejection
        if wick_ratio > 0.6:
            return True

    # 3. Check if price dropped after the volume spike
    if max_vol_idx < len(pre_bars) - 1:
        post_spike_closes = np.array([c["close"] for c in pre_bars[max_vol_idx + 1:]])
        if len(post_spike_closes) > 0 and spike_candle["high"] > 0:
            avg_post_close = np.mean(post_spike_closes)
            drop_from_high = (avg_post_close / spike_candle["high"] - 1) * 100
            if drop_from_high <= -blowoff_price_reversal_pct:
                return True

    # 4. Also check the first sustain bar: if price opened significantly
    # below the pre-sustain high, it's a reversal
    if len(sustain_bars_list) > 0:
        first_sus_open = sustain_bars_list[0]["open"]
        pre_high = max(c["high"] for c in pre_bars)
        if pre_high > 0:
            drop_to_sus = (first_sus_open / pre_high - 1) * 100
            if drop_to_sus <= -blowoff_price_reversal_pct:
                return True

    return False


def _bar(rows, idx):
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


async def run_backtest(
    db_path: str,
    overrides: dict | None = None,
    use_blowoff_filter: bool = False,
    blowoff_volume_mult: float = 5.0,
    blowoff_price_reversal_pct: float = 3.0,
    blowoff_lookback: int = 8,
) -> dict:
    """Run backtest with optional blow-off top filter."""
    cfg = Settings.from_yaml("config/config.yaml")
    if overrides:
        for k, v in overrides.items():
            if hasattr(cfg.strategy, k):
                setattr(cfg.strategy, k, v)
            elif hasattr(cfg.trading, k):
                setattr(cfg.trading, k, v)

    db_path = Path(db_path)
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    sf = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with sf() as s:
        stmt = (select(Candle.symbol, Candle.timestamp, Candle.open, Candle.high,
                       Candle.low, Candle.close, Candle.volume)
                .order_by(Candle.symbol, Candle.timestamp))
        result = await s.execute(stmt)
        rows = result.all()

    syms: dict[str, list] = {}
    for r in rows:
        syms.setdefault(r[0], []).append(r)

    timestamps = sorted(set(r[1] for r in rows))
    if not timestamps:
        await engine.dispose()
        return {}

    detector = SetupDetector(cfg.strategy, timeframe=cfg.collectors.timeframe)
    tcfg = cfg.trading

    positions: list[SimPosition] = []
    closed: list[SimPosition] = []
    sigs = 0
    blowoff_rejected = 0

    # Load OI
    oi_cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    async with sf() as s:
        oi_stmt = (select(OpenInterest.exchange, OpenInterest.symbol,
                          OpenInterest.timestamp, OpenInterest.value)
                   .order_by(OpenInterest.exchange, OpenInterest.symbol,
                             OpenInterest.timestamp))
        oi_result = await s.execute(oi_stmt)
        for ex, sym, ts, val in oi_result.all():
            oi_cache.setdefault((ex, sym), []).append((ts, val))

    need = detector.config.baseline_bars + detector.config.sustain_bars
    for ts_idx, ts in enumerate(timestamps):
        if ts_idx < need:
            continue

        # Position management (same as before)
        for pos in list(positions):
            if pos.closed:
                continue
            sd = syms.get(pos.symbol)
            if not sd:
                continue
            cb = None
            for r in sd:
                if r[1] == ts:
                    cb = r
                    break
            if not cb:
                continue

            hi, lo, cl = cb[3], cb[4], cb[5]

            if not pos.partial_closed:
                trig = pos.entry_price + (pos.tp_price - pos.entry_price) * (tcfg.partial_close_pct / 100)
                if hi >= trig:
                    cq = pos.quantity / 2
                    pos.partial_pnl = (trig - pos.entry_price) * cq
                    pos.quantity -= cq
                    pos.partial_closed = True
                    pos.sl_price = pos.entry_price
                    continue

            if hi >= pos.tp_price:
                pos.exit_price = pos.tp_price
                pos.exit_time = ts
                pos.exit_reason = "tp"
                pos.pnl = (pos.tp_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed.append(pos)
                positions.remove(pos)
                continue

            if lo <= pos.sl_price:
                pos.exit_price = pos.sl_price
                pos.exit_time = ts
                pos.exit_reason = "sl"
                pos.pnl = (pos.sl_price - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed.append(pos)
                positions.remove(pos)
                continue

            age = (ts - pos.entry_time).total_seconds() / 3600
            if age >= tcfg.max_hold_hours:
                pos.exit_price = cl
                pos.exit_time = ts
                pos.exit_reason = "time"
                pos.pnl = (cl - pos.entry_price) * pos.quantity + pos.partial_pnl
                pos.closed = True
                closed.append(pos)
                positions.remove(pos)

        if ts_idx % 3 != 0:
            continue
        if len(positions) >= tcfg.max_positions:
            continue

        for sym, sd in syms.items():
            if len(positions) >= tcfg.max_positions:
                break
            bi = -1
            for i, r in enumerate(sd):
                if r[1] == ts:
                    bi = i
                    break
            if bi < need:
                continue

            base = sym.split("/")[0].upper()
            if base in detector._exclude_coins:
                continue
            if any(p.symbol == sym for p in positions):
                continue

            candle_slice = []
            for j in range(bi - need - 15, bi + 1):  # extra bars for blowoff check
                b = sd[j] if 0 <= j < len(sd) else None
                if b:
                    candle_slice.append({
                        "open": b[2], "high": b[3],
                        "low": b[4], "close": b[5], "volume": b[6],
                    })

            if len(candle_slice) < need:
                continue

            if not detector.check_volume_pattern(candle_slice):
                continue
            if detector.check_price_trend(candle_slice) != "long":
                continue

            # NEW: Blow-off top filter
            if use_blowoff_filter:
                if check_blowoff_top(
                    candle_slice,
                    sustain_bars=detector.config.sustain_bars,
                    lookback_bars=blowoff_lookback,
                    blowoff_volume_mult=blowoff_volume_mult,
                    blowoff_price_reversal_pct=blowoff_price_reversal_pct,
                ):
                    blowoff_rejected += 1
                    continue

            # OI
            oi_ok = False
            for ex in ("bybit", "binance"):
                key = (ex, sym)
                if key not in oi_cache:
                    continue
                pts = [v for t, v in oi_cache[key] if t <= ts]
                if len(pts) < OI_TREND_BARS:
                    continue
                slope = calculate_oi_slope_pct(np.array(pts[-OI_TREND_BARS:]))
                if slope is not None and slope >= detector.config.oi_slope_min_pct:
                    oi_ok = True
                    break
            if not oi_ok:
                continue

            sigs += 1
            ep = candle_slice[-1]["close"]
            sd_ = ep * (tcfg.stop_loss_pct / 100)
            td_ = sd_ * tcfg.risk_reward_ratio
            qty = (1000 * (tcfg.risk_per_trade_pct / 100)) / sd_
            positions.append(SimPosition(
                symbol=sym, entry_price=ep, entry_time=ts,
                quantity=qty, tp_price=ep + td_, sl_price=ep - sd_,
            ))

    # Close remaining
    for pos in positions:
        sd = syms.get(pos.symbol)
        if sd:
            pos.exit_price = sd[-1][5]
            pos.exit_time = timestamps[-1]
            pos.exit_reason = "eod"
            pos.pnl = (pos.exit_price - pos.entry_price) * pos.quantity + pos.partial_pnl
        pos.closed = True
        closed.append(pos)

    wins = sum(1 for t in closed if t.pnl > 0)
    losses = sum(1 for t in closed if t.pnl <= 0)
    total_pnl = sum(t.pnl for t in closed)
    wr = (wins / len(closed) * 100) if closed else 0
    tp_w = sum(1 for t in closed if t.exit_reason == "tp")
    sl_l = sum(1 for t in closed if t.exit_reason == "sl")
    tm_e = sum(1 for t in closed if t.exit_reason == "time")

    gross_profit = sum(t.pnl for t in closed if t.pnl > 0)
    gross_loss = abs(sum(t.pnl for t in closed if t.pnl < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    await engine.dispose()

    return {
        "signals": sigs,
        "trades": len(closed),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wr, 1),
        "total_pnl": round(total_pnl, 2),
        "profit_factor": round(pf, 2),
        "tp_wins": tp_w,
        "sl_losses": sl_l,
        "time_exits": tm_e,
        "partials": sum(1 for t in closed if t.partial_closed),
        "blowoff_rejected": blowoff_rejected,
    }


async def main():
    DB_BAD = "data/trading_bot_07.06-16.06.db"
    DB_GOOD = "data/trading_bot_20.05-27.05.db"

    print("=" * 100)
    print("  BLOW-OFF TOP FILTER TEST")
    print("=" * 100)

    # Baseline
    print("\n--- BASELINE (no filter) ---")
    t0 = time.time()
    r = await run_backtest(DB_BAD, {}, use_blowoff_filter=False)
    print(f"  [{time.time()-t0:.0f}s] BAD:  T={r['trades']} WR={r['win_rate']}% "
          f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
          f"TP={r['tp_wins']} SL={r['sl_losses']} Sig={r['signals']}")

    t0 = time.time()
    r2 = await run_backtest(DB_GOOD, {}, use_blowoff_filter=False)
    print(f"  [{time.time()-t0:.0f}s] GOOD: T={r2['trades']} WR={r2['win_rate']}% "
          f"PnL=${r2['total_pnl']:+.2f} PF={r2['profit_factor']} "
          f"TP={r2['tp_wins']} SL={r2['sl_losses']} Sig={r2['signals']}")

    # Blow-off filter variations
    print("\n--- BLOW-OFF FILTER SWEEP ON BAD DB ---")
    configs = [
        # (label, blowoff_volume_mult, blowoff_price_reversal_pct, blowoff_lookback, overrides)
        ("blowoff=x5/-3%/8b", 5.0, 3.0, 8, {}),
        ("blowoff=x4/-2%/8b", 4.0, 2.0, 8, {}),
        ("blowoff=x3/-2%/10b", 3.0, 2.0, 10, {}),
        ("blowoff=x3/-3%/12b", 3.0, 3.0, 12, {}),
        ("blowoff=x5/-5%/8b", 5.0, 5.0, 8, {}),
        # Combined with parameter tightening
        ("blowoff=x4/-3%/8b + vol_x20", 4.0, 3.0, 8, {"volume_surge_mult": 20.0}),
        ("blowoff=x4/-3%/8b + vol_x20 + exh_2", 4.0, 3.0, 8,
         {"volume_surge_mult": 20.0, "exhaustion_gain_pct": 2.0}),
        ("blowoff=x4/-3%/8b + vol_x20 + oi_3", 4.0, 3.0, 8,
         {"volume_surge_mult": 20.0, "oi_slope_min_pct": 3.0}),
        ("blowoff=x4/-3%/8b + vol_x20 + exh2 + oi3", 4.0, 3.0, 8,
         {"volume_surge_mult": 20.0, "exhaustion_gain_pct": 2.0, "oi_slope_min_pct": 3.0}),
    ]

    results_bad = []
    results_good = []
    for label, bvm, bprp, blb, overrides in configs:
        print(f"\n{label}...", flush=True)
        t0 = time.time()
        r = await run_backtest(
            DB_BAD, overrides,
            use_blowoff_filter=True,
            blowoff_volume_mult=bvm,
            blowoff_price_reversal_pct=bprp,
            blowoff_lookback=blb,
        )
        elapsed = time.time() - t0
        pnl_str = f"${r['total_pnl']:+.2f}" if r['total_pnl'] != 0 else "$0.00"
        print(f"  [{elapsed:.0f}s] BAD:  T={r['trades']} WR={r['win_rate']}% "
              f"PnL={pnl_str} PF={r['profit_factor']} "
              f"TP={r['tp_wins']} SL={r['sl_losses']} "
              f"BlowoffRej={r['blowoff_rejected']} Sig={r['signals']}")
        results_bad.append((label, r))

        # Cross-validate on GOOD
        t0 = time.time()
        r2 = await run_backtest(
            DB_GOOD, overrides,
            use_blowoff_filter=True,
            blowoff_volume_mult=bvm,
            blowoff_price_reversal_pct=bprp,
            blowoff_lookback=blb,
        )
        elapsed = time.time() - t0
        pnl_str = f"${r2['total_pnl']:+.2f}" if r2['total_pnl'] != 0 else "$0.00"
        print(f"  [{elapsed:.0f}s] GOOD: T={r2['trades']} WR={r2['win_rate']}% "
              f"PnL={pnl_str} PF={r2['profit_factor']} "
              f"TP={r2['tp_wins']} SL={r2['sl_losses']} "
              f"BlowoffRej={r2['blowoff_rejected']} Sig={r2['signals']}")
        results_good.append((label, r2))

    # Summary
    print("\n" + "=" * 100)
    print("  SUMMARY - BAD DB (07.06-16.06)")
    print("=" * 100)
    for label, r in results_bad:
        pnl_str = f"${r['total_pnl']:+.2f}"
        print(f"  {label:<50s} T={r['trades']:>2d} WR={r['win_rate']:>5.1f}% "
              f"PnL={pnl_str:>8s} PF={r['profit_factor']:>5.2f} "
              f"Rej={r['blowoff_rejected']:>2d}")

    print("\n" + "=" * 100)
    print("  SUMMARY - GOOD DB (20.05-27.05) for comparison")
    print("=" * 100)
    for label, r in results_good:
        pnl_str = f"${r['total_pnl']:+.2f}"
        print(f"  {label:<50s} T={r['trades']:>2d} WR={r['win_rate']:>5.1f}% "
              f"PnL={pnl_str:>8s} PF={r['profit_factor']:>5.2f} "
              f"Rej={r['blowoff_rejected']:>2d}")

    # Best on BAD
    best_bad = max(results_bad, key=lambda x: x[1]["total_pnl"])
    print(f"\n  🏆 BEST on BAD: {best_bad[0]} → "
          f"T={best_bad[1]['trades']} WR={best_bad[1]['win_rate']}% "
          f"PnL=${best_bad[1]['total_pnl']:+.2f} PF={best_bad[1]['profit_factor']}")

    # Check which config preserves GOOD performance best
    best_good = max(results_good, key=lambda x: x[1]["total_pnl"])
    print(f"  🏆 BEST on GOOD: {best_good[0]} → "
          f"T={best_good[1]['trades']} WR={best_good[1]['win_rate']}% "
          f"PnL=${best_good[1]['total_pnl']:+.2f} PF={best_good[1]['profit_factor']}")


if __name__ == "__main__":
    asyncio.run(main())
