import numpy as np, pandas as pd
DAT = "../dat"
def load_R(ps=("P1","P2","P3","P4")):
    return pd.concat([pd.read_pickle(f"{DAT}/{p}_R.pkl") for p in ps])

def episodes(R, mask, cooldown=3600):
    """per-symbol sequential take with position+cooldown, no slots/CB."""
    S = R[mask].sort_values(["sym", "ts"])
    keep = []
    last_sym = None; busy_until = -1
    for idx, sym, ts, ex in zip(S.index, S.sym.to_numpy(), S.ts.to_numpy(), S.exit_ts.to_numpy()):
        if sym != last_sym:
            last_sym = sym; busy_until = -1
        if ts <= busy_until:   # in position (ts<exit) or exit within cooldown
            continue
        keep.append(idx)
        busy_until = ex + cooldown
    return R.loc[keep]

def summ(E):
    c = E.cls.value_counts()
    tp, sl = c.get("tp",0), c.get("sl",0)
    return dict(n=len(E), tp=tp, sl=sl, be=c.get("be",0), tm=c.get("time-",0)+c.get("time+",0),
                ratio=round(tp/max(sl,1),2), ER=round(E.R.mean(),3), take=round(100*tp/max(len(E),1),1))
