import numpy as np, numba as nb, pandas as pd
import rep

@nb.njit(cache=True)
def find_entries(o, h, l, c, g, gend, ref, mode, x, nbars):
    """mode 0: delay x bars -> close[g+x]; 1: breakout buy-stop at ref*(1+x%) within nbars;
    2: limit buy at ref*(1-x%) within nbars; 3: next-bar-close-above-ref confirmation (x ignored).
    Returns (entry_bar, entry_price) with -1 if not entered."""
    n = len(g)
    eb = np.full(n, -1, np.int64); ep = np.zeros(n)
    for k in range(n):
        gi = g[k]
        if mode == 0:
            j = gi + int(x)
            if j < gend[k]:
                eb[k] = j; ep[k] = c[j]
        elif mode == 1:
            trig = ref[k] * (1 + x / 100)
            for j in range(gi + 1, min(gi + 1 + nbars, gend[k])):
                if h[j] >= trig:
                    eb[k] = j; ep[k] = max(trig, o[j]); break
        elif mode == 2:
            lim = ref[k] * (1 - x / 100)
            for j in range(gi + 1, min(gi + 1 + nbars, gend[k])):
                if l[j] <= lim:
                    eb[k] = j; ep[k] = min(lim, o[j]); break
        elif mode == 3:
            j = gi + 1
            if j < gend[k] and c[j] > ref[k]:
                eb[k] = j; ep[k] = c[j]
    return eb, ep

def run_variant(d, S, mode, x, nbars, ex=None):
    """S: rows of one period (must have g,k,entry_ref). returns outcome frame for entered rows."""
    e = {**rep.EXIT, **(ex or {})}
    g = S.g.to_numpy(np.int64); gend = d["offs"][S.k.to_numpy()+1].astype(np.int64)
    ref = S.entry_ref.to_numpy()
    eb, ep = find_entries(d["o"], d["h"], d["l"], d["c"], g, gend, ref, mode, float(x), int(nbars))
    m = eb >= 0
    ent = ep[m] * (1 + e["slip"]/100)
    exg, kind, R, mfe, mae = rep.sim_many(d["ts"], d["h"], d["l"], d["c"], eb[m], gend[m], ent,
        np.full(m.sum(), e["sl"]), e["rr"], e["trig"], e["qty"], e["hold_h"]*3600, e["taker"], e["maker"],
        e["trail_act"], e["trail_dist"], e["tp_on"])
    out = pd.DataFrame({"kind": kind, "R": R, "mfe": mfe, "exit_ts": d["ts"][exg], "entry_ts": d["ts"][eb[m]]}, index=S.index[m])
    out["cls"] = rep.classify(out)
    return out
