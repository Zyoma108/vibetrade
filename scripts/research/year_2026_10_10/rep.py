"""Research replica of SetupDetector gates + engine position/portfolio simulation.

Used ONLY for exploration. Every candidate change is re-validated on the real
engine (src/backtest/engine.py) with rule-3 parity before being proposed.
"""
import numpy as np, pandas as pd, numba as nb, bisect

DAT = "/private/tmp/claude-501/-Users-rsentsov-Develop-vibetrade/3450d44b-78f2-4e6d-9a16-d0b1873069ae/scratchpad/dat"
PERIODS = ["P1", "P2", "P3", "P4"]

EXCLUDE = None


def exclude_set():
    global EXCLUDE
    if EXCLUDE is None:
        import yaml
        cfg = yaml.safe_load(open("/Users/rsentsov/Develop/vibetrade/config/config.yaml"))
        EXCLUDE = set(x.upper() for x in cfg["strategy"]["exclude_coins"])
    return EXCLUDE


def load(period):
    z = np.load(f"{DAT}/{period}_candles.npz", allow_pickle=True)
    d = {k: z[k] for k in z.files}
    d["period"] = period
    mc = pd.read_parquet(f"{DAT}/{period}_mc.parquet")
    d["mc_ts"] = mc["ts"].to_numpy(np.int64)
    d["mc_regime"] = mc["regime"].to_numpy(object)
    d["mc_st"] = mc["supertrend_color"].to_numpy(object)
    d["all_ts"] = np.unique(d["ts"])
    return d


def regime_at(d, ts_arr):
    if len(d["mc_ts"]) == 0:
        return np.full(len(ts_arr), "unknown", dtype=object), np.full(len(ts_arr), "green", dtype=object)
    idx = np.searchsorted(d["mc_ts"], ts_arr, side="right") - 1
    reg = np.where(idx >= 0, d["mc_regime"][np.clip(idx, 0, None)], "unknown")
    st = np.where(idx >= 0, d["mc_st"][np.clip(idx, 0, None)], "green")
    return reg, st


def _roll_med(x, w):
    return pd.Series(x).rolling(w).median().to_numpy()


def features(d, min_ratio=2.0):
    """Per-bar features for windows ending at e (all bars), then candidate rows at i.

    Row i keeps features of window e=i ("u_*") and window e=i-1 ("s_*").
    Only rows where the volume surge is at least `min_ratio` x baseline on one of
    the two windows are kept (research superset of the production gate)."""
    out = []
    offs = d["offs"]
    excl = exclude_set()
    for k, sym in enumerate(d["syms"]):
        if sym.split("/")[0].upper() in excl:
            continue
        a, b = offs[k], offs[k + 1]
        n = b - a
        if n < 75:
            continue
        o, h, l, c, v, ts = (d[x][a:b] for x in ("o", "h", "l", "c", "v", "ts"))
        rv = _roll_med(v, 70)
        rc = _roll_med(c, 70)
        e = np.arange(n)
        src = np.maximum(e - 14, 69)
        base_v = np.where(e >= 73, rv[np.minimum(src, n - 1)], np.nan)
        base_c = np.where(e >= 73, rc[np.minimum(src, n - 1)], np.nan)
        L = np.minimum(e + 1, 84)  # window length
        # sustain
        def lag(x, k_):
            r = np.full(n, np.nan); r[k_:] = x[:n - k_]; return r
        v0, v1, v2, v3 = lag(v, 3), lag(v, 2), lag(v, 1), v  # recent[0..3]
        rec = np.vstack([v0, v1, v2, v3])
        rmin = np.nanmin(rec, axis=0) if True else None
        rmax = np.max(rec, axis=0)
        rmed = np.median(rec, axis=0)
        prev_avg = (v0 + v1 + v2) / 3
        hh = np.vstack([lag(h, 3), lag(h, 2), lag(h, 1), h]).max(axis=0)
        ll = np.vstack([lag(l, 3), lag(l, 2), lag(l, 1), l]).min(axis=0)
        o3 = lag(o, 3)
        with np.errstate(all="ignore"):
            change = (c / o3 - 1) * 100
            rng = (hh / ll - 1) * 100
            pre = np.where(L >= 84, (lag(c, 4) / lag(o, 13) - 1) * 100, np.nan)
            c19 = lag(c, 19)
            lo20 = pd.Series(c).rolling(20).min().to_numpy()
            hdrop = np.where(L >= 20, (lo20 / c19 - 1) * 100, np.nan)
            cpos = np.where(h - l > 0, (c - l) / (h - l), np.nan)
            ext = (hh / base_c - 1) * 100
            retr = (hh - c) / hh * 100
            ratio_min = rmin / base_v
            surge = np.mean(rec, axis=0) / base_v
            liq = base_v * base_c
        F = pd.DataFrame({
            "base_v": base_v, "liq": liq, "ratio_min": ratio_min, "spike": rmax / rmed,
            "fade": v3 / prev_avg, "decl": v3 / v0, "surge": surge,
            "change": change, "range": rng, "pre": pre, "hdrop": hdrop, "cpos": cpos,
            "ext": ext, "retr": retr, "close": c,
        })
        F["valid"] = e >= 73
        # candidate rows: window i or window i-1 shows surge >= min_ratio
        rm_prev = np.concatenate([[np.nan], ratio_min[:-1]])
        keep = np.where(((ratio_min >= min_ratio) | (rm_prev >= min_ratio)) & (e >= 74))[0]
        if len(keep) == 0:
            continue
        U = F.iloc[keep].add_prefix("u_").reset_index(drop=True)
        S = F.iloc[keep - 1].add_prefix("s_").reset_index(drop=True)
        R = pd.concat([U, S], axis=1)
        R["sym"] = sym
        R["k"] = k
        R["i"] = keep
        R["g"] = a + keep
        R["ts"] = ts[keep]
        out.append(R)
    X = pd.concat(out, ignore_index=True)
    reg, st = regime_at(d, X["ts"].to_numpy())
    X["regime"] = reg
    X["st"] = st
    return X


DEFAULT = dict(mult=5.0, cautious_inc=50.0, min_liq=25000.0, smooth=5.0, fading=0.7, declining=True,
               pre_max=3.0, hdrop=10.0, gmin=1.0, rng=4.0, ex_gain=5.0, ex_pos=0.7, ext=10.0,
               retr=0.0, gmax=12.0)


def vol_stage(X, p, pre, mult):
    """0 pass, 1 silent, 2 oversized, 3 undersized (mirrors check_volume_pattern)."""
    st = np.ones(len(X), dtype=np.int8)
    bv = X[pre + "base_v"].to_numpy()
    ok = (X[pre + "valid"].to_numpy()) & (bv > 0)
    if p["min_liq"] > 0:
        ok &= X[pre + "liq"].to_numpy() >= p["min_liq"]
    ok &= X[pre + "ratio_min"].to_numpy() >= mult
    spike = X[pre + "spike"].to_numpy() > p["smooth"]
    fade = (X[pre + "fade"].to_numpy() < p["fading"]) if p["fading"] > 0 else np.zeros(len(X), bool)
    decl = (X[pre + "decl"].to_numpy() < 1.0) if p["declining"] else np.zeros(len(X), bool)
    st[ok] = 0
    st[ok & spike] = 2
    st[ok & ~spike & fade] = 3
    st[ok & ~spike & ~fade & decl] = 3
    return st


def price_pass(X, p, pre):
    """Returns (pass bool array, reject stage str array) mirroring check_price_trend order."""
    n = len(X)
    stage = np.array([""] * n, dtype=object)
    alive = np.ones(n, bool)

    def rej(mask, name):
        nonlocal alive
        m = alive & mask
        stage[m] = name
        alive = alive & ~mask

    g = lambda c: X[pre + c].to_numpy()
    with np.errstate(invalid="ignore"):
        if p["pre_max"] > 0:
            rej(np.nan_to_num(g("pre"), nan=-1e9) > p["pre_max"], "pre_surge_pump")
        if p["hdrop"] > 0:
            rej(np.nan_to_num(g("hdrop"), nan=0) <= -p["hdrop"], "hourly_drop")
        rej(g("change") < p["gmin"], "price_growth_low")
        if p["rng"] > 0:
            rej(g("range") > p["rng"], "window_range")
        if p["ex_gain"] > 0:
            rej((g("change") > p["ex_gain"]) & (np.nan_to_num(g("cpos"), nan=0) > p["ex_pos"]), "exhaustion")
        if p["ext"] > 0:
            rej(g("ext") > p["ext"], "exhaustion_extreme")
        if p["retr"] > 0:
            rej(g("retr") > p["retr"], "retracement")
        if p["gmax"] > 0:
            rej(g("change") > p["gmax"], "price_growth_high")
    return alive, stage


def gate(X, p=None, extra=None):
    """Production-equivalent signal decision per row. Returns DataFrame cols:
    sig (bool), shifted (bool), stage (reject reason), entry_ref (close of window end)."""
    p = {**DEFAULT, **(p or {})}
    caut = (X["regime"].to_numpy() == "cautious")
    res = {}
    for mult_name, mult in (("n", p["mult"]), ("c", p["mult"] * (1 + p["cautious_inc"] / 100))):
        su = vol_stage(X, p, "u_", mult)
        ss = vol_stage(X, p, "s_", mult)
        use_u = su == 0
        use_s = (su == 1) & (ss == 0) & X["s_valid"].to_numpy()
        res[mult_name] = (use_u, use_s, su, ss)
    use_u = np.where(caut, res["c"][0], res["n"][0])
    use_s = np.where(caut, res["c"][1], res["n"][1])
    su = np.where(caut, res["c"][2], res["n"][2])
    pu, stu = price_pass(X, p, "u_")
    ps, sts = price_pass(X, p, "s_")
    if extra is not None:
        eu, es = extra(X, "u_"), extra(X, "s_")
        stu = np.where(pu & ~eu, "extra", stu); pu = pu & eu
        sts = np.where(ps & ~es, "extra", sts); ps = ps & es
    sig = (use_u & pu) | (use_s & ps)
    stage = np.where(use_u, stu, np.where(use_s, sts, np.where(su == 2, "volume_spike", np.where(su == 3, "volume_undersized", "silent"))))
    entry_ref = np.where(use_s, X["s_close"].to_numpy(), X["u_close"].to_numpy())
    return pd.DataFrame({"sig": sig, "shifted": use_s & ~use_u, "stage": stage, "entry_ref": entry_ref,
                         "volwin": use_u | use_s}, index=X.index)


@nb.njit(cache=True)
def sim_one(ts, h, l, c, g0, gend, entry, sl_pct, rr, trig_pct, qty_pct, hold_s, taker, maker,
            trail_act, trail_dist, tp_on, pess=False, tslip=0.0):
    """Single position, engine order of checks. Returns (exit_g, kind, R, mfe, mae).
    kind: 0 tp, 1 sl, 2 time, 3 eod. R per 1 unit of risk, net of fees.
    trail_act/trail_dist: optional trailing stop after price reaches +trail_act% (0=off)."""
    sl_d = entry * sl_pct / 100
    q = 1.0 / sl_d
    tp = entry + sl_d * rr
    sl = entry - sl_d
    fee = q * entry * taker / 100
    partial = False
    ppnl = 0.0
    t0 = ts[g0]
    mfe = 0.0
    mae = 0.0
    peak = entry
    for j in range(g0 + 1, gend):
        hi = h[j]; lo = l[j]
        if (hi / entry - 1) * 100 > mfe:
            mfe = (hi / entry - 1) * 100
        if (lo / entry - 1) * 100 < mae:
            mae = (lo / entry - 1) * 100
        if trig_pct > 0 and not partial:
            trig = entry + (tp - entry) * trig_pct / 100
            if hi >= trig:
                cq = q * qty_pct / 100
                ppnl = (trig - entry) * cq
                q -= cq
                partial = True
                if sl < entry:
                    sl = entry
                fee += trig * cq * maker / 100
                if hi > peak:
                    peak = hi
                continue
        if tp_on and hi >= tp:
            fee += tp * q * maker / 100
            return j, 0, (tp - entry) * q + ppnl - fee, mfe, mae
        if lo <= sl:
            px = sl * (1 - tslip / 100) if sl > entry else sl
            fee += px * q * taker / 100
            return j, 1, (px - entry) * q + ppnl - fee, mfe, mae
        if hi > peak:
            peak = hi
        if trail_act > 0 and peak >= entry * (1 + trail_act / 100):
            ns = peak - entry * trail_dist / 100
            if ns > sl:
                sl = ns
                if pess and lo <= sl:
                    px = sl * (1 - tslip / 100)
                    fee += px * q * taker / 100
                    return j, 1, (px - entry) * q + ppnl - fee, mfe, mae
        if ts[j] - t0 >= hold_s:
            fee += c[j] * q * taker / 100
            return j, 2, (c[j] - entry) * q + ppnl - fee, mfe, mae
    j = gend - 1
    fee += c[j] * q * taker / 100
    return j, 3, (c[j] - entry) * q + ppnl - fee, mfe, mae


@nb.njit(cache=True)
def sim_many(ts, h, l, c, gs, gends, entries, sl_pct, rr, trig_pct, qty_pct, hold_s, taker, maker,
             trail_act, trail_dist, tp_on, pess=False, tslip=0.0):
    n = len(gs)
    ex = np.empty(n, np.int64); kind = np.empty(n, np.int8); R = np.empty(n); mfe = np.empty(n); mae = np.empty(n)
    for k in range(n):
        a, b, r, m1, m2 = sim_one(ts, h, l, c, gs[k], gends[k], entries[k], sl_pct[k], rr, trig_pct, qty_pct,
                                  hold_s, taker, maker, trail_act, trail_dist, tp_on, pess, tslip)
        ex[k] = a; kind[k] = b; R[k] = r; mfe[k] = m1; mae[k] = m2
    return ex, kind, R, mfe, mae


EXIT = dict(sl=5.0, rr=2.0, trig=35.0, qty=20.0, hold_h=72.0, taker=0.055, maker=0.02, slip=0.015,
            trail_act=0.0, trail_dist=0.0, tp_on=True, pess=False, tslip=0.0)


def outcomes(d, X, rows, ex=None, sl_arr=None):
    """Simulate positions for X.loc[rows] (boolean or index), entry = entry_ref*(1+slip)."""
    e = {**EXIT, **(ex or {})}
    sub = X.loc[rows]
    gs = sub["g"].to_numpy(np.int64)
    gends = d["offs"][sub["k"].to_numpy() + 1].astype(np.int64)
    entries = sub["entry_ref"].to_numpy() * (1 + e["slip"] / 100)
    sl = np.full(len(sub), e["sl"]) if sl_arr is None else np.asarray(sl_arr, float)
    exg, kind, R, mfe, mae = sim_many(d["ts"], d["h"], d["l"], d["c"], gs, gends, entries, sl, e["rr"], e["trig"],
                                      e["qty"], e["hold_h"] * 3600, e["taker"], e["maker"], e["trail_act"],
                                      e["trail_dist"], e["tp_on"], e["pess"], e["tslip"])
    out = pd.DataFrame({"exit_g": exg, "kind": kind, "R": R, "mfe": mfe, "mae": mae,
                        "exit_ts": d["ts"][exg], "exit_px": d["c"][exg]}, index=sub.index)
    # exit price per engine: tp/sl at level, else close
    ent = entries
    tp = ent * (1 + sl / 100 * e["rr"]); slp = ent * (1 - sl / 100)
    out["entry"] = ent
    out["tp_px"] = tp
    trig_abs = e["sl"] * e["rr"] * e["trig"] / 100 if e["trig"] > 0 else 1e9
    out["partial"] = out["mfe"].to_numpy() >= (sl * e["rr"] * e["trig"] / 100 if e["trig"] > 0 else 1e9) - 1e-9
    return out


def classify(o, X=None):
    """full take / breakeven / stop / time+ / time- per project conventions (price-based)."""
    k = o["kind"].to_numpy()
    R = o["R"].to_numpy()
    cls = np.where(k == 0, "tp", np.where(k == 1, np.where(R < -0.5, "sl", "be"), np.where(R > 0, "time+", "time-")))
    return cls


def portfolio(d, X, sig_mask, out, max_pos=10, cooldown_h=1.0, cb=True, cb_reduce=2, cb_stop=3, cb_min=60,
              block=True):
    """Engine-equivalent portfolio pass. X rows with sig_mask are candidates at their ts.
    `out` = outcomes() for those rows (same index). Returns DataFrame of taken trades."""
    C = X.loc[sig_mask, ["sym", "ts", "regime", "st"]].join(out[["exit_ts", "R", "kind", "partial"]])
    C = C.sort_values(["ts", "sym"], kind="stable")
    by_ts = {}
    for row in C.itertuples():
        by_ts.setdefault(row.ts, []).append(row)
    all_ts = d["all_ts"]
    need = 74
    open_pos = {}  # sym -> (exit_ts, R, kind, entry_ts)
    last_exit = {}
    taken = []
    cb_losses = 0; cb_until = None; cb_consumed = 0
    cand_ts = np.array(sorted(by_ts.keys()))
    # iterate over timestamps where something can happen: candidate ts and exit ts
    events = set(cand_ts.tolist())
    exits_at = {}
    cool = cooldown_h * 3600
    pending_exit_ts = []
    import heapq
    for t in sorted(events):
        pass
    # simple full pass over all_ts restricted to relevant ones
    heap = []
    idx_need = all_ts[need] if len(all_ts) > need else all_ts[-1]
    for t in cand_ts:
        # process exits up to and including t
        while heap and heap[0][0] <= t:
            et, sym = heapq.heappop(heap)
            pe = open_pos.pop(sym)
            last_exit[sym] = et
            if cb:
                if pe[2] == 3:
                    pass  # eod: engine does not touch CB
                else:
                    r = pe[1]
                    if pe[2] == 0 or r > 0:
                        cb_losses = 0; cb_until = None; cb_consumed = 0
                    elif pe[3]:
                        pass  # neutral: stop already at breakeven (cb_trade_outcome)
                    else:
                        cb_losses += 1
                        if cb_losses >= cb_stop and cb_losses > cb_consumed:
                            start = cb_until if (cb_until is not None and cb_until > et) else et
                            cb_until = start + cb_min * 60; cb_consumed = cb_losses
        if t < idx_need:
            continue
        if len(open_pos) >= max_pos:
            continue
        rows = by_ts[t]
        if block and (rows[0].regime == "risk_off" or (rows[0].regime == "cautious" and rows[0].st == "red")):
            continue
        cb_mult = 1.0
        if cb:
            if cb_until is not None:
                if t < cb_until:
                    continue
                cb_until = None
            if cb_losses >= cb_reduce:
                cb_mult = 0.5
        for r in rows:
            if len(open_pos) >= max_pos:
                break
            if r.sym in open_pos:
                continue
            le = last_exit.get(r.sym)
            if cooldown_h > 0 and le is not None and le >= t - cool:
                continue
            open_pos[r.sym] = (r.exit_ts, r.R, r.kind, bool(r.partial), t)
            heapq.heappush(heap, (r.exit_ts, r.sym))
            taken.append((r.Index, r.sym, t, r.exit_ts, r.R, r.kind, cb_mult * (0.5 if r.regime == "cautious" else 1.0)))
    T = pd.DataFrame(taken, columns=["row", "sym", "ts", "exit_ts", "R", "kind", "size"]).set_index("row")
    return T
