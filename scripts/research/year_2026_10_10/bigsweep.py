import sys, pickle, json; sys.path.insert(0,".")
import numpy as np, pandas as pd, rep
from multiprocessing import Pool
HM = ["2025-10","2025-11","2025-12","2026-01","2026-02","2026-03","2026-04","2026-05","2026-06","2026-07","2026-08","2026-09"]
DB = ["P1","P2","P3","P4"]
DAYS = {"P1":14.8,"P2":26.1,"P3":6.6,"P4":12.1}
import cfgs; CFGS = cfgs.CFGS
def load(p):
    if p in DB:
        d = rep.load(p); X = pd.read_pickle(f"../dat/{p}_X.pkl")
        X = X.drop(columns=[c for c in ["sig","shifted","stage","entry_ref","volwin"] if c in X.columns])
        days = DAYS[p]
    else:
        d = pickle.load(open(f"../hist/D_{p}.pkl","rb")); X = pd.read_pickle(f"../hist/X_{p}.pkl")
        days = (d["win"][1]-d["win"][0])/86400
    return d, X, days
def work(p):
    d, X, days = load(p)
    out = {}
    for label, gp, ex, port in CFGS:
        G = rep.gate(X, gp); sig = G.sig.to_numpy(); XX = X.join(G)
        o = rep.outcomes(d, XX, sig, ex=ex)
        T = rep.portfolio(d, XX, sig, o, **(port or {}))
        cls = rep.classify(T)
        T["day"] = T.ts // 86400
        out[label] = dict(n=len(T), days=days, tp=int((cls=="tp").sum()), sl=int((cls=="sl").sum()),
                          SR=float(T.R.sum()), daily=T.groupby("day").R.sum().to_dict(),
                          keys={f"{s}|{t}": r for s, t, r in zip(T.sym, T.ts, T.R)})
    return p, out
if __name__ == "__main__":
    import cfgs; CFGS = cfgs.CFGS
    res = {}
    with Pool(6) as pool:
        for p, out in pool.imap_unordered(work, HM + DB):
            res[p] = out; print("done", p, flush=True)
    pickle.dump(res, open(sys.argv[1], "wb"))
