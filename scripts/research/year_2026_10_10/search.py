import sys; sys.path.insert(0,".")
import pandas as pd, numpy as np, itertools
E = pd.read_pickle("../dat/ALL_E.pkl")
E = E[~E.blocked]
feats = [c for c in E.columns if c.startswith(("w_","x_","m_","rs_","oi_"))]
P = ["P1","P2","P3","P4"]
per = E.period.to_numpy()
def conds():
    out = []
    for f in feats:
        x = E[f].to_numpy(float)
        qs = np.nanquantile(x, [0.2,0.4,0.6,0.8])
        for q in qs:
            out.append((f"{f}<={q:.3g}", x <= q)); out.append((f"{f}>{q:.3g}", x > q))
    return out
C = conds()
def evaluate(y_tp, y_sl, min_frac=0.25):
    hits = []
    for (n1, m1) in C:
        ok = True; rs=[]
        for p in P:
            mp = m1 & (per==p)
            tp, sl = (y_tp & mp).sum(), (y_sl & mp).sum()
            if sl == 0 or tp/sl < 1.0 or mp.sum() < min_frac*(per==p).sum(): ok=False; break
            rs.append(round(tp/sl,2))
        if ok: hits.append((n1, rs))
    hits2 = []
    for (n1,m1),(n2,m2) in itertools.combinations(C, 2):
        if n1.split("<")[0].split(">")[0] == n2.split("<")[0].split(">")[0]: continue
        m = m1 & m2; ok=True; rs=[]
        for p in P:
            mp = m & (per==p)
            tp, sl = (y_tp & mp).sum(), (y_sl & mp).sum()
            if sl == 0 or tp/sl < 1.0 or mp.sum() < min_frac*(per==p).sum(): ok=False; break
            rs.append(round(tp/sl,2))
        if ok: hits2.append((n1+" & "+n2, rs))
    return hits, hits2
cls = E.cls.to_numpy()
tp, sl = cls=="tp", cls=="sl"
mf = float(sys.argv[1]) if len(sys.argv)>1 else 0.25
h1, h2 = evaluate(tp, sl, mf)
print("REAL: single", len(h1), "pairs", len(h2), "of", len(C), "conds")
for h in h1[:20]: print("  ", h)
for h in sorted(h2, key=lambda x: -min(x[1]))[:25]: print("  ", h)
rng = np.random.default_rng(1)
for it in range(3):
    c2 = cls.copy()
    for p in P:
        idx = np.where(per==p)[0]; c2[idx] = rng.permutation(c2[idx])
    a, b = evaluate(c2=="tp", c2=="sl", mf)
    print("SHUFFLED", it, "single", len(a), "pairs", len(b))
