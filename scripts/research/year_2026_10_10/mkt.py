import numpy as np, pandas as pd, sqlite3, rep
def market_frame(d):
    """per-ts market aggregates over all symbols (incl. majors)."""
    rows = []
    syms = d["syms"]; offs = d["offs"]
    sym_idx = np.repeat(np.arange(len(syms)), np.diff(offs))
    df = pd.DataFrame({"s": sym_idx, "ts": d["ts"], "c": d["c"], "v": d["v"]})
    C = df.pivot(index="ts", columns="s", values="c").sort_index()
    V = df.pivot(index="ts", columns="s", values="v").sort_index()
    C = C.ffill(limit=5)
    out = pd.DataFrame(index=C.index)
    for w, name in [(20,"1h"),(80,"4h"),(480,"24h")]:
        r = (C / C.shift(w) - 1) * 100
        out["m_r"+name] = r.median(axis=1)
        out["m_up"+name] = (r > 2).mean(axis=1)
    med = V.rolling(70, min_periods=35).median().shift(14)
    surge = (V.rolling(4).min() >= 5 * med)
    out["m_surge_now"] = surge.sum(axis=1)
    out["m_surge1h"] = surge.rolling(20, min_periods=1).max().sum(axis=1)
    out["m_surge6h"] = surge.rolling(120, min_periods=1).max().sum(axis=1)
    # BTC
    for nm in ["BTC/USDT:USDT"]:
        if nm in list(syms):
            k = list(syms).index(nm)
            out["btc_r1h"] = ((C[k]/C[k].shift(20)-1)*100)
            out["btc_r24h"] = ((C[k]/C[k].shift(480)-1)*100)
    return out, C
