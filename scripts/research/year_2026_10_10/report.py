import pickle, sys, numpy as np, pandas as pd
res = pickle.load(open(sys.argv[1],"rb"))
HM = sorted(p for p in res if p.startswith("20")); DB = ["P1","P2","P3","P4"]
labels = list(res[HM[0]].keys())
rng = np.random.default_rng(0)
def daily_delta(lbl, ps):
    # paired per-day difference vs base, block bootstrap over days
    diffs=[]
    for p in ps:
        a = res[p][lbl]["daily"]; b = res[p]["base"]["daily"]
        days = set(a)|set(b)
        diffs += [a.get(k,0)-b.get(k,0) for k in days]
        # days with no trades in either count as 0 diff (omitted—same effect on sum)
    diffs=np.array(diffs); tot=diffs.sum()
    if len(diffs)==0: return 0,(0,0)
    bs=[rng.choice(diffs,len(diffs)).sum() for _ in range(2000)]
    return tot, tuple(np.round(np.percentile(bs,[2.5,97.5]),1))
rows=[]
for l in labels:
    Y = [res[p][l] for p in HM]; D=[res[p][l] for p in DB]
    yt=sum(x["tp"] for x in Y); ys=sum(x["sl"] for x in Y); yR=sum(x["SR"] for x in Y); yn=sum(x["n"] for x in Y); ydays=sum(x["days"] for x in Y)
    pos=sum(x["SR"]>0 for x in Y)
    dt=sum(x["tp"] for x in D); ds=sum(x["sl"] for x in D); dR=sum(x["SR"] for x in D); dn=sum(x["n"] for x in D)
    dy,ciy = daily_delta(l, HM); dd,cid = daily_delta(l, DB)
    rows.append(dict(cfg=l, Y_tpd=round(yn/ydays,2), Y_ratio=round(yt/max(ys,1),2), Y_R=round(yR,1), Y_pos=f"{pos}/12",
                     Y_dR=f"{dy:+.1f} {ciy}{'*' if ciy[0]>0 or ciy[1]<0 else ''}",
                     DB_tpd=round(dn/59.6,2), DB_ratio=round(dt/max(ds,1),2), DB_R=round(dR,1),
                     DB_dR=f"{dd:+.1f} {cid}{'*' if cid[0]>0 or cid[1]<0 else ''}",
                     DB_per=" ".join(f"{res[p][l]['SR']:+.0f}" for p in DB)))
pd.set_option("display.width",300)
print(pd.DataFrame(rows).to_string(index=False))
