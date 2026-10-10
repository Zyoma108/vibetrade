import sys; sys.path.insert(0,".")
import numpy as np, pandas as pd, rep, hist, flush, fam
m = sys.argv[1]
d = hist.make_period(m); s, e = d["win"]
cfgs = [(10,10),(12,10),(15,10),(12,20),(15,20),(15,40),(20,20),(10,80),(15,80)]
rows=[]
for drop, w in cfgs:
    flush._ev.clear()
    X = flush.base_events(d, drop, w)
    X = X[(X.ts>=s)&(X.ts<e)]
    X = X[d["usd24"][X.g.to_numpy()] >= 1e6]
    o = rep.outcomes(d, X, X.index); X = X.join(o); X["cls"] = rep.classify(o)
    X["cfg"]=f"d{drop}w{w}"; X["period"]=m; rows.append(X)
Xr = fam.events(d, "random"); Xr = Xr[(Xr.ts>=s)&(Xr.ts<e)]
Xr = Xr[d["usd24"][Xr.g.to_numpy()] >= 1e6]
o = rep.outcomes(d, Xr, Xr.index); Xr = Xr.join(o); Xr["cls"] = rep.classify(o); Xr["cfg"]="random"; Xr["period"]=m
rows.append(Xr)
pd.concat(rows).to_pickle(f"../hist/F_{m}.pkl"); print(m, "ok", flush=True)
