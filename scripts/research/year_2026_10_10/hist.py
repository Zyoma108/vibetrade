import numpy as np, pandas as pd, glob, os
H = "../hist"
def month_bounds(m):
    s = pd.Timestamp(m + "-01")
    e = s + pd.offsets.MonthBegin(1)
    return int(s.timestamp()), int(e.timestamp())
def make_period(m, pre_days=2, post_days=3, min_usd24=1e6):
    s, e = month_bounds(m)
    lo, hi = s - pre_days*86400, e + post_days*86400
    syms=[]; parts={k:[] for k in ["ts","o","h","l","c","v","tbv","qv"]}; offs=[0]
    for f in sorted(glob.glob(f"{H}/*.npz")):
        z = np.load(f)
        ts = z["ts"]; m_ = (ts >= lo) & (ts < hi) & (z["v"] > 0)
        if m_.sum() < 600: continue
        for k in parts: parts[k].append(z[k][m_])
        base = os.path.basename(f)[:-8]  # strip USDT.npz
        syms.append(f"{base}/USDT:USDT"); offs.append(offs[-1] + int(m_.sum()))
    d = {k: np.concatenate(v) for k, v in parts.items()}
    d["syms"] = np.array(syms, dtype=object); d["offs"] = np.array(offs)
    d["period"] = m; d["mc_ts"] = np.array([], np.int64); d["mc_regime"] = np.array([], object); d["mc_st"] = np.array([], object)
    d["all_ts"] = np.unique(d["ts"]); d["win"] = (s, e)
    # 24h quote volume per bar (bot scans only coins with >= 1M daily volume)
    usd24 = np.empty(len(d["ts"]))
    for k in range(len(syms)):
        a, b = offs[k], offs[k+1]
        usd24[a:b] = pd.Series(d["qv"][a:b]).rolling(480, min_periods=1).sum().to_numpy()
    d["usd24"] = usd24
    return d
