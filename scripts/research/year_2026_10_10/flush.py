import sys; sys.path.insert(0,".")
import numpy as np, pandas as pd, rep
def lag(x,k):
    r=np.full(len(x),np.nan); r[k:]=x[:len(x)-k]; return r
_ev = {}
def base_events(d, drop=12, w=20, gap=80, min_liq=25000):
    """first bar where close fell >= drop% vs max close over previous w bars (peak-to-now)."""
    key=(d["period"],drop,w,gap,min_liq)
    if key in _ev: return _ev[key]
    rows=[]; offs=d["offs"]; excl=rep.exclude_set()
    for k,sym in enumerate(d["syms"]):
        if sym.split("/")[0].upper() in excl: continue
        a,b=offs[k],offs[k+1]; n=b-a
        if n<600: continue
        o,h,l,c,v=(d[x][a:b] for x in "ohlcv")
        usd = pd.Series(v*c).rolling(70).median().shift(w).to_numpy()
        with np.errstate(all="ignore"):
            pk = pd.Series(h).rolling(w).max().to_numpy()
            dd = (c/pk-1)*100
            r4h = (lag(c,w)/lag(c,w+80)-1)*100      # trend before the flush
            r24 = (lag(c,w)/lag(c,w+480)-1)*100
            vm = pd.Series(v).rolling(w).sum().to_numpy()/ (pd.Series(v).rolling(200).median().shift(w).to_numpy()*w)
            lowpos = (c - pd.Series(l).rolling(w).min().to_numpy())/(pk - pd.Series(l).rolling(w).min().to_numpy())
            atr = pd.Series((h-l)/c*100).rolling(200).mean().shift(w).to_numpy()
            r1 = (c/o-1)*100
        m = (dd <= -drop) & (usd >= min_liq)
        m[:600]=False
        idx=np.where(m)[0]; last=-10**9
        for i in idx:
            if i-last<gap: last=i; continue
            last=i
            rows.append((k,i,a+i,d["ts"][a+i],c[i],dd[i],r4h[i],r24[i],vm[i],lowpos[i],atr[i],r1[i],usd[i]))
    X=pd.DataFrame(rows,columns=["k","i","g","ts","entry_ref","dd","r4h","r24","vm","lowpos","atr","r1","usd"])
    X["sym"]=d["syms"][X.k]
    _ev[key]=X
    return X
def evalfam(drop=12, w=20, ex=None, filt=None, label="", ps=("P1","P2","P3","P4"), verbose=True):
    out=[]; agg=[0,0,0.0,0]
    res={}
    for p in ps:
        d=rep.load(p) if p not in _d else _d[p]; _d[p]=d
        X=base_events(d,drop,w)
        if filt is not None: X = X[filt(X)]
        o=rep.outcomes(d,X,X.index,ex=ex); cls=rep.classify(o)
        tp,sl=(cls=="tp").sum(),(cls=="sl").sum()
        res[p]=(len(X),tp,sl,o.R.sum(),o)
        out.append(f"{p}: n={len(X):4d} {tp:3d}:{sl:3d} r={tp/max(sl,1):.2f} ER={o.R.mean():+.3f}")
        agg[0]+=tp; agg[1]+=sl; agg[2]+=o.R.sum(); agg[3]+=len(X)
    if verbose: print(f"{label:30s}", " | ".join(out), f"|| r={agg[0]/max(agg[1],1):.2f} ER={agg[2]/max(agg[3],1):+.3f}", flush=True)
    return res
_d={}
