import sys; sys.path.insert(0,".")
import numpy as np, pandas as pd, rep, hist, regime, pickle
for m in sys.argv[1:]:
    d = hist.make_period(m)
    B = np.load("../hist_btc/BTC.npz")
    RG = regime.regimes(regime.alt_index_hourly(d), B["ts"], B["c"])
    d["mc_ts"] = RG.ts.to_numpy(np.int64); d["mc_regime"] = RG.regime.to_numpy(object); d["mc_st"] = RG.st.to_numpy(object)
    s, e = d["win"]
    X = rep.features(d)
    X = X[(X.ts >= s) & (X.ts < e)]
    X = X[d["usd24"][X.g.to_numpy()] >= 1e6]
    X.to_pickle(f"../hist/X_{m}.pkl")
    keep = {k: d[k] for k in ["syms","offs","ts","o","h","l","c","mc_ts","mc_regime","mc_st","all_ts","win"]}
    keep["v"] = d["v"]; keep["period"] = m
    with open(f"../hist/D_{m}.pkl","wb") as f: pickle.dump(keep, f, protocol=5)
    print(m, len(X), flush=True)
