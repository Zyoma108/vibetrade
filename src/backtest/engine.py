"""Движок бэктеста — единственная реализация цикла симуляции.

До 26.08.2026 их было три: `run_backtest()` здесь же в runner.py, копия в
`scripts/sweep_retracement.py` (быстрая, на ней стояли все свипы) и полностью
протухшая в `scripts/sweep_rr_sl.py`. Совпадали они на 62% строк, а расходились
в существенном: shift-ретрай детектора был только в свипе, `oi_filter_enabled` —
только в runner. Это структурная причина уже случившихся parity-багов: когда
`oi_declining` отсутствовал в скриптах, он завысил результаты свипов по RR,
partial-close и retracement, и это выяснилось сильно позже принятых решений.

Теперь цикл один. `runner.py` — отчётная обёртка над ним, свипы — параметризация.
Любой новый фильтр детектора попадает сюда автоматически: `simulate()` вызывает
`SetupDetector.check_volume_pattern`/`check_price_trend` вместо того, чтобы
повторять их условия.

Поиск свечи по timestamp — O(1) через dict, а не линейный скан: на 3.8 млн свечей
и ~600 символах старый runner не укладывался в разумное время.
"""

import bisect
import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np

from src.analytics.detector import VOLUME_REVERSAL_STAGES, SetupDetector
from src.analytics.utils import OI_TREND_BARS, calculate_oi_slope_pct, timeframe_to_minutes


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


class PendingEntry:
    """Лимитник на вход, ожидающий отката от цены сигнала (pending_entry_pullback_pct)."""

    __slots__ = (
        "symbol", "limit_price", "signal_time", "expires_at",
        "quantity", "tp_price", "sl_price", "fee",
    )

    def __init__(self, symbol: str, limit_price: float, signal_time: datetime,
                 expires_at: datetime, quantity: float, tp_price: float,
                 sl_price: float, fee: float):
        self.symbol = symbol
        self.limit_price = limit_price
        self.signal_time = signal_time
        self.expires_at = expires_at
        self.quantity = quantity
        self.tp_price = tp_price
        self.sl_price = sl_price
        self.fee = fee  # комиссия входа, известна заранее (maker, лимит известен)


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


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _parse_ts_cache(distinct_strs):
    cache = {}
    for s in distinct_strs:
        if s is None:
            continue
        try:
            if "." in s:
                cache[s] = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
            else:
                cache[s] = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # fallback for any other formatting sqlite might have produced
            cache[s] = datetime.fromisoformat(s)
    return cache


def load_data(db_path: str, limit_days: float | None = None, limit_symbols: list[str] | None = None):
    db = sqlite3.connect(db_path)
    where = []
    params = []
    if limit_days is not None:
        row = db.execute("SELECT MAX(timestamp) FROM candles").fetchone()
        max_ts = row[0]
        where.append("timestamp >= datetime(?, ?)")
        params.extend([max_ts, f"-{limit_days} days"])
    if limit_symbols:
        placeholders = ",".join("?" for _ in limit_symbols)
        where.append(f"symbol IN ({placeholders})")
        params.extend(limit_symbols)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    t0 = time.time()
    rows = db.execute(
        f"SELECT symbol, timestamp, open, high, low, close, volume FROM candles {where_sql} "
        f"ORDER BY symbol, timestamp",
        params,
    ).fetchall()
    log(f"candles fetched: {len(rows)} rows in {time.time()-t0:.1f}s")

    distinct_ts = sorted(set(r[1] for r in rows))
    ts_cache = _parse_ts_cache(distinct_ts)

    symbols: dict[str, list] = {}
    for sym, ts, o, h, l, c, v in rows:
        symbols.setdefault(sym, []).append((ts_cache[ts], o, h, l, c, v))

    # per-symbol index structures for O(1) lookup
    sym_ts_to_row: dict[str, dict] = {}
    sym_ts_to_idx: dict[str, dict] = {}
    for sym, data in symbols.items():
        sym_ts_to_row[sym] = {r[0]: r for r in data}
        sym_ts_to_idx[sym] = {r[0]: i for i, r in enumerate(data)}

    all_timestamps = sorted(ts_cache.values())
    log(f"symbols: {len(symbols)} | distinct timestamps: {len(all_timestamps)}")

    # OI cache: (exchange, symbol) -> [(ts, value), ...] sorted by ts
    t0 = time.time()
    oi_where = []
    oi_params = []
    if limit_days is not None:
        row = db.execute("SELECT MAX(timestamp) FROM open_interest").fetchone()
        if row[0] is not None:
            oi_where.append("timestamp >= datetime(?, ?)")
            oi_params.extend([row[0], f"-{limit_days} days"])
    if limit_symbols:
        placeholders = ",".join("?" for _ in limit_symbols)
        oi_where.append(f"symbol IN ({placeholders})")
        oi_params.extend(limit_symbols)
    oi_where_sql = ("WHERE " + " AND ".join(oi_where)) if oi_where else ""
    oi_rows = db.execute(
        f"SELECT exchange, symbol, timestamp, value FROM open_interest {oi_where_sql} "
        f"ORDER BY exchange, symbol, timestamp",
        oi_params,
    ).fetchall()
    oi_distinct_ts = sorted(set(r[2] for r in oi_rows))
    oi_ts_cache = _parse_ts_cache(oi_distinct_ts)
    oi_cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    for ex, sym, ts, val in oi_rows:
        oi_cache.setdefault((ex, sym), []).append((oi_ts_cache[ts], val))
    log(f"OI: {len(oi_rows)} rows, {len(oi_cache)} symbol-keys in {time.time()-t0:.1f}s")

    # Market context snapshots
    mc_rows = db.execute(
        "SELECT timestamp, regime, supertrend_color FROM market_context_snapshots ORDER BY timestamp"
    ).fetchall()
    mc_ts_cache = _parse_ts_cache([r[0] for r in mc_rows])
    mc_snapshots = [(mc_ts_cache[r[0]], r[1], r[2]) for r in mc_rows]
    mc_ts_list = [x[0] for x in mc_snapshots]
    log(f"MarketContext snapshots: {len(mc_snapshots)}")

    db.close()
    return {
        "symbols": symbols,
        "sym_ts_to_row": sym_ts_to_row,
        "sym_ts_to_idx": sym_ts_to_idx,
        "all_timestamps": all_timestamps,
        "oi_cache": oi_cache,
        "mc_snapshots": mc_snapshots,
        "mc_ts_list": mc_ts_list,
    }


def _regime_at(data, ts):
    mc_ts_list = data["mc_ts_list"]
    if not mc_ts_list:
        return "unknown", "green"
    idx = bisect.bisect_right(mc_ts_list, ts) - 1
    if idx < 0:
        return "unknown", "green"
    _, regime, st_color = data["mc_snapshots"][idx]
    return regime, st_color


def compute_retracement_pct(candle_slice, sustain):
    highs = [c["high"] for c in candle_slice[-sustain:]]
    window_high = max(highs) if highs else 0.0
    if window_high <= 0:
        return None
    return (window_high - candle_slice[-1]["close"]) / window_high * 100


def simulate(settings, data, has_oi: bool = True, collect_retracement: bool = True):
    cfg = settings.trading
    detector = SetupDetector(settings.strategy, timeframe=settings.collectors.timeframe)

    symbols = data["symbols"]
    sym_ts_to_row = data["sym_ts_to_row"]
    sym_ts_to_idx = data["sym_ts_to_idx"]
    all_timestamps = data["all_timestamps"]
    oi_cache = data["oi_cache"]
    has_mc = len(data["mc_snapshots"]) > 0

    positions: list[SimPosition] = []
    pending: list[PendingEntry] = []
    closed_trades: list[SimPosition] = []
    signals_count = 0
    pending_filled = 0
    pending_expired = 0

    # id(pos) -> retracement_pct at signal time (kept out-of-band; SimPosition uses __slots__)
    retr_map: dict[int, float] = {}
    price_stage_counts: dict[str, int] = {}
    vol_stage_counts: dict[str, int] = {}
    shift_used_count = 0  # сколько сигналов найдено только благодаря сдвигу на -1 свечу

    cb_losses = 0
    cb_stop_until = None
    cb_stop_consumed_at = 0

    # symbol -> last exit_time (O(1) cooldown check instead of O(n) scan of closed_trades)
    last_exit_time: dict[str, datetime] = {}

    need_bars = detector.config.baseline_bars + detector.config.sustain_bars
    sustain = detector.config.sustain_bars

    # Каданс поиска новых сигналов синхронизирован с реальной скоростью коллектора
    # (settings.collectors.scan_cycle_seconds), а не захардкожен — см. runner.py
    timeframe_minutes = timeframe_to_minutes(settings.collectors.timeframe)
    cycle_delay_bars = max(
        1, round(settings.collectors.scan_cycle_seconds / (timeframe_minutes * 60))
    )

    for ts_idx, ts in enumerate(all_timestamps):
        if ts_idx < need_bars:
            continue

        # Рыночный режим запрещает ТОЛЬКО новые входы. Раньше здесь стоял
        # `continue`, который пропускал весь такт целиком — вместе с сопровождением
        # уже открытых позиций и pending-лимитников. В risk_off-окне позиции
        # переставали проверяться на TP/SL и на max_hold_hours и «замерзали» до
        # конца окна. В проде так не бывает: `update_positions()` идёт каждый цикл,
        # а `block_entries` влияет только на `open_position()`.
        entries_blocked = False
        if has_mc:
            regime, st_color = _regime_at(data, ts)
            entries_blocked = regime == "risk_off" or (
                regime == "cautious" and st_color == "red"
            )
        else:
            regime, st_color = "unknown", "green"

        # --- manage open positions ---
        for pos in list(positions):
            if pos.closed:
                continue
            row = sym_ts_to_row.get(pos.symbol, {}).get(ts)
            if row is None:
                continue
            _, o, high, low, close, v = row

            if not pos.partial_closed:
                trigger = pos.entry_price + (pos.tp_price - pos.entry_price) * (cfg.partial_close_pct / 100)
                if high >= trigger:
                    close_qty = pos.quantity * (getattr(cfg, "partial_close_qty_pct", 50.0) / 100)
                    partial_pnl = (trigger - pos.entry_price) * close_qty
                    pos.quantity -= close_qty
                    pos.partial_closed = True
                    pos.partial_pnl = partial_pnl
                    pos.sl_price = pos.entry_price
                    pos.fee += _fee(cfg, trigger * close_qty, taker=False)
                    continue

            if high >= pos.tp_price:
                pos.exit_price = pos.tp_price
                pos.exit_time = ts
                pos.exit_reason = "tp"
                # tp_as_limit_order (прод-дефолт) → TP реально maker, не taker
                pos.fee += _fee(cfg, pos.tp_price * pos.quantity, taker=not cfg.tp_as_limit_order)
                pos.pnl = (pos.tp_price - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                last_exit_time[pos.symbol] = ts
                if cfg.circuit_breaker_enabled:
                    cb_losses = 0
                    cb_stop_until = None
                    cb_stop_consumed_at = 0
                continue

            if low <= pos.sl_price:
                pos.exit_price = pos.sl_price
                pos.exit_time = ts
                pos.exit_reason = "sl"
                pos.fee += _fee(cfg, pos.sl_price * pos.quantity, taker=True)
                pos.pnl = (pos.sl_price - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
                pos.closed = True
                closed_trades.append(pos)
                positions.remove(pos)
                last_exit_time[pos.symbol] = ts
                if cfg.circuit_breaker_enabled:
                    cb_losses += 1
                continue

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
                last_exit_time[pos.symbol] = ts
                if cfg.circuit_breaker_enabled:
                    if pos.pnl > 0:
                        cb_losses = 0
                        cb_stop_until = None
                        cb_stop_consumed_at = 0
                    else:
                        cb_losses += 1

        # --- manage pending entries ---
        for pe in list(pending):
            row = sym_ts_to_row.get(pe.symbol, {}).get(ts)
            if row is None:
                continue
            _, o, high, low, close, v = row
            if low <= pe.limit_price:
                pos = SimPosition(
                    symbol=pe.symbol, entry_price=pe.limit_price, entry_time=ts,
                    quantity=pe.quantity, tp_price=pe.tp_price, sl_price=pe.sl_price,
                    fee=pe.fee,
                )
                if id(pe) in retr_map:
                    retr_map[id(pos)] = retr_map.pop(id(pe))
                positions.append(pos)
                pending.remove(pe)
                pending_filled += 1
                continue
            if ts >= pe.expires_at:
                pending.remove(pe)
                pending_expired += 1
                retr_map.pop(id(pe), None)

        if ts_idx % cycle_delay_bars != 0:
            continue
        if len(positions) + len(pending) >= cfg.max_positions:
            continue

        if entries_blocked:
            continue

        cb_mult = 1.0
        if cfg.circuit_breaker_enabled:
            if cb_stop_until is not None:
                if ts < cb_stop_until:
                    continue
                cb_stop_until = None
            if cb_losses >= cfg.circuit_breaker_loss_streak_stop and cb_losses > cb_stop_consumed_at:
                cb_stop_until = ts + timedelta(minutes=cfg.circuit_breaker_stop_minutes)
                cb_stop_consumed_at = cb_losses
                continue
            if cb_losses >= cfg.circuit_breaker_loss_streak_reduce:
                cb_mult = cfg.circuit_breaker_reduce_mult_pct / 100.0

        if regime == "cautious":
            increase_pct = detector.config.cautious_volume_surge_mult_increase_pct
            detector.apply_regime_multiplier(1.0 + increase_pct / 100.0)
        else:
            detector.apply_regime_multiplier(1.0)

        positioned_symbols = {p.symbol for p in positions} | {pe.symbol for pe in pending}

        for sym, sym_data in symbols.items():
            if len(positions) + len(pending) >= cfg.max_positions:
                break

            base = sym.split("/")[0].upper()
            if base in getattr(detector, "_exclude_coins", set()):
                continue
            if sym in positioned_symbols:
                continue

            cooldown_cutoff = ts - timedelta(hours=cfg.cooldown_hours)
            if cfg.cooldown_hours > 0:
                last_exit = last_exit_time.get(sym)
                if last_exit is not None and last_exit >= cooldown_cutoff:
                    continue

            bar_idx = sym_ts_to_idx.get(sym, {}).get(ts, -1)
            if bar_idx < 0 or bar_idx < need_bars:
                continue

            candle_slice = []
            for j in range(bar_idx - need_bars - 10, bar_idx + 1):
                bar = _bar(sym_data, j)
                if bar:
                    candle_slice.append({
                        "open": bar[1], "high": bar[2],
                        "low": bar[3], "close": bar[4], "volume": bar[5],
                    })
            if len(candle_slice) < need_bars:
                continue

            # Volume pattern с тем же shift-ретраем, что и в SetupDetector.analyze()
            # (см. src/analytics/detector.py) — воспроизведено 1:1, чтобы можно было
            # свипом измерить реальный вклад сдвига в сигналы/PnL, а не гадать вслепую.
            vol_window = None
            vol_ctx: dict = {}
            if detector.check_volume_pattern(candle_slice, vol_ctx):
                vol_window = candle_slice
            elif vol_ctx.get("stage") not in VOLUME_REVERSAL_STAGES:
                shifted = candle_slice[:-1]
                shift_ctx: dict = {}
                if len(shifted) >= need_bars and detector.check_volume_pattern(shifted, shift_ctx):
                    vol_window = shifted
                    shift_used_count += 1
                elif shift_ctx:
                    vol_ctx = shift_ctx

            if vol_window is None:
                if vol_ctx.get("stage"):
                    vol_stage_counts[vol_ctx["stage"]] = vol_stage_counts.get(vol_ctx["stage"], 0) + 1
                continue

            retracement_pct = None
            if collect_retracement:
                retracement_pct = compute_retracement_pct(vol_window, sustain)

            price_ctx: dict = {}
            direction = detector.check_price_trend(vol_window, price_ctx)
            if direction != "long":
                if price_ctx.get("stage"):
                    price_stage_counts[price_ctx["stage"]] = price_stage_counts.get(price_ctx["stage"], 0) + 1
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
                    # oi_declining (см. SetupDetector._check_oi_trend): последняя точка OI
                    # ниже предпоследней → приток уже иссякает, блок независимо от порога
                    # наклона. Раньше отсутствовало здесь — искусственно завышало число
                    # сигналов относительно боевой логики. Флаг читается из того же
                    # конфига, что и в детекторе, чтобы порог можно было свипать.
                    if (
                        detector.config.oi_declining_enabled
                        and len(oi_vals) >= 2
                        and oi_vals[-1] < oi_vals[-2]
                    ):
                        continue
                    slope_pct = calculate_oi_slope_pct(oi_vals)
                    if slope_pct is not None and slope_pct >= detector.config.oi_slope_min_pct:
                        oi_pass = True
                        break
                if not oi_pass:
                    continue

            signals_count += 1
            signal_price = vol_window[-1]["close"]

            # regime здесь уже не risk_off и не cautious+ST=red (см. блокировку выше),
            # поэтому "cautious" тут всегда соответствует реальному position_size_mult=0.5
            virtual_balance = 1000.0
            regime_size_mult = 0.5 if regime == "cautious" else 1.0
            risk_budget = (
                virtual_balance * (cfg.risk_per_trade_pct / 100) * cb_mult * regime_size_mult
            )

            if cfg.pending_entry_pullback_pct > 0:
                limit_price = signal_price * (1 - cfg.pending_entry_pullback_pct / 100)
                sl_distance = limit_price * (cfg.stop_loss_pct / 100)
                tp_distance = sl_distance * cfg.risk_reward_ratio
                qty = risk_budget / sl_distance
                tp = limit_price + tp_distance
                sl = limit_price - sl_distance
                entry_fee = _fee(cfg, qty * limit_price, taker=False)
                expires_at = ts + timedelta(minutes=cfg.pending_entry_timeout_minutes)
                pe = PendingEntry(
                    symbol=sym, limit_price=limit_price, signal_time=ts,
                    expires_at=expires_at, quantity=qty, tp_price=tp, sl_price=sl,
                    fee=entry_fee,
                )
                if retracement_pct is not None:
                    retr_map[id(pe)] = retracement_pct
                pending.append(pe)
                positioned_symbols.add(sym)
                continue

            entry_price = signal_price * (1 + cfg.backtest_slippage_pct / 100)
            sl_distance = entry_price * (cfg.stop_loss_pct / 100)
            tp_distance = sl_distance * cfg.risk_reward_ratio
            qty = risk_budget / sl_distance
            tp = entry_price + tp_distance
            sl = entry_price - sl_distance
            entry_fee = _fee(cfg, qty * entry_price, taker=True)

            pos = SimPosition(
                symbol=sym, entry_price=entry_price, entry_time=ts,
                quantity=qty, tp_price=tp, sl_price=sl, fee=entry_fee,
            )
            if retracement_pct is not None:
                retr_map[id(pos)] = retracement_pct
            positions.append(pos)
            positioned_symbols.add(sym)

    # close remaining open positions at last known price
    for pos in positions:
        sym_data = symbols.get(pos.symbol)
        if sym_data:
            last_close = sym_data[-1][4]
            pos.exit_price = last_close
            pos.exit_time = all_timestamps[-1]
            pos.exit_reason = "eod"
            pos.fee += _fee(cfg, last_close * pos.quantity, taker=True)
            pos.pnl = (last_close - pos.entry_price) * pos.quantity + pos.partial_pnl - pos.fee
        pos.closed = True
        closed_trades.append(pos)

    pending_expired += len(pending)

    wins = sum(1 for t in closed_trades if t.pnl > 0)
    losses = sum(1 for t in closed_trades if t.pnl <= 0)
    total_pnl = sum(t.pnl for t in closed_trades)
    win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0
    tp_wins = sum(1 for t in closed_trades if t.exit_reason == "tp")
    sl_losses = sum(1 for t in closed_trades if t.exit_reason == "sl")
    time_exits = sum(1 for t in closed_trades if t.exit_reason == "time")
    partials = sum(1 for t in closed_trades if t.partial_closed)
    total_fees = sum(t.fee for t in closed_trades)

    # "bad" trade proxy per db-audit-august-2026: never touched partial trigger AND lost
    bad_no_partial_loss = sum(1 for t in closed_trades if (not t.partial_closed) and t.pnl <= 0)
    good_reached_partial_or_tp = sum(1 for t in closed_trades if t.partial_closed or t.exit_reason == "tp")

    trades_out = []
    for t in closed_trades:
        trades_out.append({
            "symbol": t.symbol,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat() if t.exit_time else None,
            "pnl": t.pnl,
            "exit_reason": t.exit_reason,
            "partial_closed": t.partial_closed,
            "partial_pnl": t.partial_pnl,
            "retracement_pct": retr_map.get(id(t)),
            "tp_price": t.tp_price,
            "sl_price": t.sl_price,
        })

    return {
        "signals": signals_count,
        "trades": len(closed_trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(closed_trades), 2) if closed_trades else 0,
        "total_fees": round(total_fees, 2),
        "tp_wins": tp_wins,
        "sl_losses": sl_losses,
        "time_exits": time_exits,
        "partials": partials,
        "bad_no_partial_loss": bad_no_partial_loss,
        "good_reached_partial_or_tp": good_reached_partial_or_tp,
        "pending_filled": pending_filled,
        "pending_expired": pending_expired,
        "price_stage_counts": price_stage_counts,
        "vol_stage_counts": vol_stage_counts,
        "shift_used_count": shift_used_count,
        "trades_list": trades_out,
        "period": f"{all_timestamps[0]} -> {all_timestamps[-1]}" if all_timestamps else "",
        # Ниже — то, что нужно отчёту runner.py; свипы этим не пользуются.
        "avg_fee": round(total_fees / len(closed_trades), 4) if closed_trades else 0,
        "best": max(trades_out, key=lambda t: t["pnl"]) if trades_out else None,
        "worst": min(trades_out, key=lambda t: t["pnl"]) if trades_out else None,
        "has_oi": has_oi,
        "has_mc": has_mc,
    }
