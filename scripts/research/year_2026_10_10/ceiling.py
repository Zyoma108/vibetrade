import sys; sys.path.insert(0,".")
import pandas as pd, numpy as np, itertools
exec(open("search.py").read().split("def evaluate")[0])
cls = E.cls.to_numpy(); tp, sl = cls=="tp", cls=="sl"
def best(tp, sl, mf, topk=8):
    res=[]
    perm = {p:(per==p) for p in P}
    tot = {p: perm[p].sum() for p in P}
    def score(m):
        rs=[]
        for p in P:
            mp = m & perm[p]
            if mp.sum() < mf*tot[p]: return None
            rs.append((tp&mp).sum()/max((sl&mp).sum(),1))
        return rs
    for n,m in C:
        r = score(m)
        if r: res.append((min(r), n, np.round(r,2)))
    for (n1,m1),(n2,m2) in itertools.combinations(C,2):
        r = score(m1&m2)
        if r: res.append((min(r), n1+" & "+n2, np.round(r,2)))
    res.sort(key=lambda x:-x[0])
    return res[:topk]
mf = float(sys.argv[1])
for r in best(tp, sl, mf): print("REAL", round(r[0],2), r[1], r[2])
rng = np.random.default_rng(7)
for it in range(3):
    c2 = cls.copy()
    for p in P:
        idx = np.where(per==p)[0]; c2[idx] = rng.permutation(c2[idx])
    b = best(c2=="tp", c2=="sl", mf, 1)
    print("SHUF", it, round(b[0][0],2), b[0][1], b[0][2])
