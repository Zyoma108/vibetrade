"""History pipeline per month: features -> gate -> research rows (vol-threshold) + prod portfolio."""
import sys, time; sys.path.insert(0, ".")
import numpy as np, pandas as pd, rep, ext, hist
def tb_feats(d, R):
    parts=[]
    for k, idx in R.groupby("k").groups.items():
        a, b = d["offs"][k], d["offs"][k+1]
        v, tb = d["v"][a:b], d["tbv"][a:b]
        s = lambda x, w: pd.Series(x).rolling(w, min_periods=1).sum().to_numpy()
        sub = R.loc[idx]; e = sub["i"].to_numpy() - sub["shifted"].to_numpy().astype(int)
        with np.errstate(all="ignore"):
            f = pd.DataFrame({
                "tb_sus": (s(tb,4)/s(v,4))[e],
                "tb_last": (tb/v)[e],
                "tb_pre": (s(tb,10)/s(v,10))[np.maximum(e-4,0)],
                "tb_base": (s(tb,70)/s(v,70))[np.maximum(e-14,0)],
                "tb_1h": (s(tb,20)/s(v,20))[e],
            }, index=idx)
        f["tb_sus_rel"] = f.tb_sus - f.tb_base
        parts.append(f)
    return R.join(pd.concat(parts))
for m in sys.argv[1:]:
    t0 = time.time()
    d = hist.make_period(m)
    import regime
    B = np.load("../hist_btc/BTC.npz")
    RG = regime.regimes(regime.alt_index_hourly(d), B["ts"], B["c"])
    d["mc_ts"] = RG.ts.to_numpy(np.int64); d["mc_regime"] = RG.regime.to_numpy(object); d["mc_st"] = RG.st.to_numpy(object)
    s, e = d["win"]
    X = rep.features(d)
    X = X[(X.ts >= s) & (X.ts < e)]
    X["usd24"] = d["usd24"][X.g.to_numpy()]
    X = X[X.usd24 >= 1e6]
    G = rep.gate(X); X = X.join(G)
    sig = X.sig.to_numpy()
    o = rep.outcomes(d, X, sig)
    T = rep.portfolio(d, X, sig, o)
    T = T.join(o[["mfe","mae","partial"]]); T["cls"] = rep.classify(T)
    T.to_pickle(f"../hist/T_{m}.pkl")
    thr_u = (X.u_ratio_min >= 5) & (X.u_liq >= 25000) & X.u_valid
    thr_s = (X.s_ratio_min >= 5) & (X.s_liq >= 25000) & X.s_valid
    R = X[thr_u | thr_s].copy()
    R = ext.ext_features(d, R); R = tb_feats(d, R)
    oR = rep.outcomes(d, R, R.index); R = R.join(oR); R["cls"] = rep.classify(oR)
    R["period"] = m; R["blocked"] = (R.regime == "risk_off") | ((R.regime == "cautious") & (R.st == "red"))
    for f in ["ratio_min","spike","fade","decl","surge","change","range","pre","hdrop","cpos","ext","retr","liq"]:
        R["w_"+f] = np.where(R.shifted, R["s_"+f], R["u_"+f])
    R.to_pickle(f"../hist/R_{m}.pkl")
    c = T.cls.value_counts()
    print(m, f"trades {len(T)} tpd {len(T)/((e-s)/86400):.2f} tp {c.get('tp',0)} sl {c.get('sl',0)} ΣR {T.R.sum():+.1f} | research rows {len(R)} | {time.time()-t0:.0f}s", flush=True)
