"""
Test improved filters on both databases:
1. Market breadth filter — blocks longs when majority of coins are declining
2. Extended price context — checks total pump beyond sustain window
3. Volume climax detection for pre-sustain candles

Usage:
    python scripts/test_improved_filters.py
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


def check_market_breadth(all_symbols_data: dict, ts: datetime,
                         lookback_bars: int = 20,
                         min_pct_up: float = 48.0) -> bool:
    """
    Check if overall market is bullish enough for long trades.
    Returns True if market is healthy (enough coins closing up).
    Returns False if market is bearish — should block entries.

    Samples all available coins at this timestamp and checks close vs open.
    """
    up_count = 0
    total = 0

    for sym, rows in all_symbols_data.items():
        # Find bar at or before ts
        best = None
        for r in rows:
            if r[1] <= ts:
                best = r
            else:
                break
        if best and best[2] > 0:  # open > 0
            total += 1
            if best[5] > best[2]:  # close > open
                up_count += 1
        if total >= 200:  # sample is enough
            break

    if total < 50:
        return True  # Not enough data — allow trading

    pct_up = (up_count / total) * 100
    return pct_up >= min_pct_up


def check_extended_price_context(candles: list[dict], sustain_bars: int,
                                 max_total_gain_pct: float = 15.0,
                                 extra_lookback: int = 8) -> bool:
    """
    Check if the TOTAL pump (including bars before sustain window) is too large.
    This catches coins that pumped hard just before the sustain window started.

    Returns True if signal should be BLOCKED (total gain too large).
    """
    if len(candles) < sustain_bars + extra_lookback:
        return False

    # Find the lowest close in the lookback + sustain period
    all_closes = np.array([c["close"] for c in candles[-(sustain_bars + extra_lookback):]])
    all_highs = np.array([c["high"] for c in candles[-(sustain_bars + extra_lookback):]])

    current_close = all_closes[-1]
    if current_close <= 0:
        return False

    # Check gain from the lowest point in the extended window
    lowest_close = np.min(all_closes[:-1])  # exclude current bar
    max_high = np.max(all_highs)

    # Total gain from lowest close to current
    total_gain = (current_close / lowest_close - 1) * 100
    if total_gain > max_total_gain_pct:
        return True

    # Also check: gain from lowest to highest point (intra-candle)
    max_gain = (max_high / lowest_close - 1) * 100
    if max_gain > max_total_gain_pct * 1.5:
        return True

    return False


def check_pre_sustain_spike(candles: list[dict], sustain_bars: int,
                            pre_lookback: int = 6,
                            spike_mult: float = 8.0,
                            reversal_pct: float = 3.0) -> bool:
    """
    Check for volume spike + reversal in bars immediately before sustain window.

    Returns True if a blow-off pattern is detected (BLOCK signal).
    """
    if len(candles) < sustain_bars + pre_lookback + 3:
        return False

    pre_bars = candles[-(sustain_bars + pre_lookback):-sustain_bars]
    sustain_bars_list = candles[-sustain_bars:]

    if len(pre_bars) < 4:
        return False

    pre_volumes = np.array([c["volume"] for c in pre_bars])
    pre_median = np.median(pre_volumes)
    if pre_median <= 0:
        return False

    max_vol = np.max(pre_volumes)
    if max_vol / pre_median < spike_mult:
        return False

    # The spike candle
    max_idx = np.argmax(pre_volumes)
    spike = pre_bars[max_idx]

    # Check: long upper wick (>60% of range)
    candle_range = spike["high"] - spike["low"]
    if candle_range > 0:
        upper_wick = spike["high"] - max(spike["open"], spike["close"])
        if upper_wick / candle_range > 0.6:
            return True

    # Check: price dropped after the spike
    if max_idx < len(pre_bars) - 1:
        post_spike = np.array([c["close"] for c in pre_bars[max_idx + 1:]])
        avg_post = np.mean(post_spike)
        if spike["high"] > 0:
            drop = (avg_post / spike["high"] - 1) * 100
            if drop <= -reversal_pct:
                return True

    # Check: first sustain bar opens well below pre-sustain high
    if sustain_bars_list:
        pre_high = max(c["high"] for c in pre_bars)
        first_sus_open = sustain_bars_list[0]["open"]
        if pre_high > 0:
            drop = (first_sus_open / pre_high - 1) * 100
            if drop <= -reversal_pct:
                return True

    return False


def _bar(rows, idx):
    if 0 <= idx < len(rows):
        return rows[idx]
    return None


async def run_improved_backtest(
    db_path: str,
    overrides: dict | None = None,
    use_breadth: bool = False,
    min_breadth_pct: float = 48.0,
    use_extended_price: bool = False,
    max_total_gain_pct: float = 15.0,
    use_pre_spike: bool = False,
    spike_mult: float = 8.0,
) -> dict:
    """Run backtest with improved filters."""
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
    breadth_rej = 0
    ext_price_rej = 0
    pre_spike_rej = 0

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

    # Cache market breadth calculations (expensive — compute every 10 bars)
    breadth_cache: dict[datetime, bool] = {}

    for ts_idx, ts in enumerate(timestamps):
        if ts_idx < need:
            continue

        # Position management
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
                trig = pos.entry_price + (pos.tp_price - pos.entry_price) * (
                    tcfg.partial_close_pct / 100)
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

        # Market breadth check (cached every 10 cycles)
        if use_breadth:
            cache_key = None
            for cached_ts in breadth_cache:
                if abs((ts - cached_ts).total_seconds()) < 300:  # 5 min cache
                    cache_key = cached_ts
                    break
            if cache_key is None:
                breadth_ok = check_market_breadth(syms, ts, min_pct_up=min_breadth_pct)
                breadth_cache[ts] = breadth_ok
            else:
                breadth_ok = breadth_cache[cache_key]

            if not breadth_ok:
                breadth_rej += 1
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
            for j in range(bi - need - 15, bi + 1):
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

            # NEW FILTER 1: Extended price context
            if use_extended_price:
                if check_extended_price_context(
                    candle_slice, detector.config.sustain_bars,
                    max_total_gain_pct=max_total_gain_pct,
                ):
                    ext_price_rej += 1
                    continue

            # NEW FILTER 2: Pre-sustain volume spike
            if use_pre_spike:
                if check_pre_sustain_spike(
                    candle_slice, detector.config.sustain_bars,
                    spike_mult=spike_mult,
                ):
                    pre_spike_rej += 1
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
        "breadth_rej": breadth_rej,
        "ext_price_rej": ext_price_rej,
        "pre_spike_rej": pre_spike_rej,
    }


async def main():
    DB_BAD = "data/trading_bot_07.06-16.06.db"
    DB_GOOD = "data/trading_bot_20.05-27.05.db"

    print("=" * 110)
    print("  IMPROVED FILTERS BACKTEST")
    print("=" * 110)

    # 1. Baseline
    print("\n--- 1. BASELINE ---")
    t0 = time.time()
    r = await run_improved_backtest(DB_BAD, {})
    print(f"  [{time.time()-t0:.0f}s] BAD:  T={r['trades']} WR={r['win_rate']}% "
          f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
          f"TP={r['tp_wins']} SL={r['sl_losses']} Sig={r['signals']}")
    t0 = time.time()
    r2 = await run_improved_backtest(DB_GOOD, {})
    print(f"  [{time.time()-t0:.0f}s] GOOD: T={r2['trades']} WR={r2['win_rate']}% "
          f"PnL=${r2['total_pnl']:+.2f} PF={r2['profit_factor']} "
          f"TP={r2['tp_wins']} SL={r2['sl_losses']} Sig={r2['signals']}")

    # 2. Market breadth only
    print("\n--- 2. MARKET BREADTH FILTER (min 48% up) ---")
    for min_pct in [48.0, 49.0, 50.0]:
        t0 = time.time()
        r = await run_improved_backtest(DB_BAD, {}, use_breadth=True, min_breadth_pct=min_pct)
        el = time.time() - t0
        rj = r.get('breadth_rej', 0)
        print(f"  [{el:.0f}s] BAD breadth>={min_pct}%: T={r['trades']} WR={r['win_rate']}% "
              f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
              f"BreadthRej={rj} Sig={r['signals']}")

    # 3. Extended price context only
    print("\n--- 3. EXTENDED PRICE CONTEXT (max total gain) ---")
    for max_gain in [10.0, 15.0, 20.0]:
        t0 = time.time()
        r = await run_improved_backtest(
            DB_BAD, {}, use_extended_price=True, max_total_gain_pct=max_gain)
        el = time.time() - t0
        rj = r.get('ext_price_rej', 0)
        print(f"  [{el:.0f}s] BAD max_gain<={max_gain}%: T={r['trades']} WR={r['win_rate']}% "
              f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
              f"ExtPriceRej={rj} Sig={r['signals']}")

    # 4. Pre-sustain spike only
    print("\n--- 4. PRE-SUSTAIN VOLUME SPIKE FILTER ---")
    for spike_m in [5.0, 8.0, 10.0]:
        t0 = time.time()
        r = await run_improved_backtest(
            DB_BAD, {}, use_pre_spike=True, spike_mult=spike_m)
        el = time.time() - t0
        rj = r.get('pre_spike_rej', 0)
        print(f"  [{el:.0f}s] BAD spike>x{spike_m}: T={r['trades']} WR={r['win_rate']}% "
              f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
              f"SpikeRej={rj} Sig={r['signals']}")

    # 5. Best combinations
    print("\n--- 5. COMBINATIONS ---")
    combos = [
        ("vol20 + breadth48", {"volume_surge_mult": 20.0}, True, 48.0, False, 15.0, False, 8.0),
        ("vol20 + extPrice15", {"volume_surge_mult": 20.0}, False, 48.0, True, 15.0, False, 8.0),
        ("vol20 + preSpike8", {"volume_surge_mult": 20.0}, False, 48.0, False, 15.0, True, 8.0),
        ("vol20 + breadth48 + extPrice15", {"volume_surge_mult": 20.0}, True, 48.0, True, 15.0, False, 8.0),
        ("vol20 + breadth48 + preSpike8", {"volume_surge_mult": 20.0}, True, 48.0, False, 15.0, True, 8.0),
        ("vol20 + breadth48 + extPrice12 + preSpike5",
         {"volume_surge_mult": 20.0}, True, 48.0, True, 12.0, True, 5.0),
        ("vol25 + breadth48 + extPrice15 + preSpike8",
         {"volume_surge_mult": 25.0}, True, 48.0, True, 15.0, True, 8.0),
        ("vol20 + breadth49 + extPrice10 + preSpike5 + exh2 + oi3",
         {"volume_surge_mult": 20.0, "exhaustion_gain_pct": 2.0, "oi_slope_min_pct": 3.0},
         True, 49.0, True, 10.0, True, 5.0),
    ]

    for label, overrides, ub, mbp, uep, mtg, ups, sm in combos:
        print(f"\n{label}...", flush=True)
        t0 = time.time()
        r = await run_improved_backtest(
            DB_BAD, overrides,
            use_breadth=ub, min_breadth_pct=mbp,
            use_extended_price=uep, max_total_gain_pct=mtg,
            use_pre_spike=ups, spike_mult=sm,
        )
        el = time.time() - t0
        print(f"  [{el:.0f}s] BAD: T={r['trades']} WR={r['win_rate']}% "
              f"PnL=${r['total_pnl']:+.2f} PF={r['profit_factor']} "
              f"TP={r['tp_wins']} SL={r['sl_losses']} "
              f"Rej: breadth={r['breadth_rej']} extPrice={r['ext_price_rej']} spike={r['pre_spike_rej']} "
              f"Sig={r['signals']}")

        # Cross-validate on GOOD
        t0 = time.time()
        r2 = await run_improved_backtest(
            DB_GOOD, overrides,
            use_breadth=ub, min_breadth_pct=mbp,
            use_extended_price=uep, max_total_gain_pct=mtg,
            use_pre_spike=ups, spike_mult=sm,
        )
        el = time.time() - t0
        print(f"  [{el:.0f}s] GOOD: T={r2['trades']} WR={r2['win_rate']}% "
              f"PnL=${r2['total_pnl']:+.2f} PF={r2['profit_factor']} "
              f"TP={r2['tp_wins']} SL={r2['sl_losses']} "
              f"Rej: breadth={r2['breadth_rej']} extPrice={r2['ext_price_rej']} spike={r2['pre_spike_rej']} "
              f"Sig={r2['signals']}")


if __name__ == "__main__":
    asyncio.run(main())
