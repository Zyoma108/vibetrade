import sys; sys.path.insert(0,".")
import numpy as np, pandas as pd, rep
def lag(x,k):
    r=np.full(len(x),np.nan); r[k:]=x[:len(x)-k]; return r
def events(d, fam, gap_bars=80, seed=0):
    rows=[]; offs=d["offs"]; excl=rep.exclude_set(); rng=np.random.default_rng(seed)
    for k,sym in enumerate(d["syms"]):
        if sym.split("/")[0].upper() in excl: continue
        a,b=offs[k],offs[k+1]; n=b-a
        if n<600: continue
        o,h,l,c,v=(d[x][a:b] for x in "ohlcv")
        usd = pd.Series(v*c).rolling(70).median().to_numpy()
        liq = usd >= 25000
        with np.errstate(all="ignore"):
            r10=(c/lag(c,10)-1)*100; r20=(c/lag(c,20)-1)*100
            hi24=lag(pd.Series(h).rolling(476).max().to_numpy(),4)
            atr=pd.Series((h-l)/c*100).rolling(80).mean().to_numpy()
            vm = v/pd.Series(v).rolling(70).median().shift(14).to_numpy()
        if fam=="flush8": m = (r20<=-8)&liq
        elif fam=="flush5": m = (r10<=-5)&liq
        elif fam=="flush12": m = (r20<=-12)&liq
        elif fam=="brk24q": m = (c>hi24)&(lag(atr,4)<0.6)&liq&(vm>3)
        elif fam=="brk24": m = (c>hi24)&liq&(vm>3)
        elif fam=="random": m = liq & (rng.random(n)<0.002)
        m[:500]=False
        idx=np.where(m)[0]; last=-10**9
        for i in idx:
            if i-last<gap_bars: continue
            last=i; rows.append((k,i,a+i,d["ts"][a+i],c[i]))
    X=pd.DataFrame(rows,columns=["k","i","g","ts","entry_ref"]); X["sym"]=d["syms"][X.k]
    return X
if __name__ == '__main__':
  for fam in sys.argv[1:]:
      out=[]
      for p in ["P1","P2","P3","P4"]:
          d=rep.load(p); X=events(d,fam)
          o=rep.outcomes(d,X,X.index); cls=rep.classify(o)
          tp,sl=(cls=="tp").sum(),(cls=="sl").sum()
          out.append(f"{p}: n={len(X)} {tp}:{sl} r={tp/max(sl,1):.2f} ER={o.R.mean():+.3f}")
      print(f"{fam:8s}", " | ".join(out), flush=True)
