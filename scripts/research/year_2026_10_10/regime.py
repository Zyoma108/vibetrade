import numpy as np, pandas as pd, glob, os
def supertrend_last(h, l, c, period=10, mult=3.0):
    tr = np.maximum(h-l, np.maximum(np.abs(h-np.roll(c,1)), np.abs(l-np.roll(c,1)))); tr[0]=h[0]-l[0]
    atr=np.zeros(len(tr)); atr[period-1]=tr[:period].mean()
    for i in range(period,len(tr)): atr[i]=(atr[i-1]*(period-1)+tr[i])/period
    hl2=(h+l)/2; up=hl2+mult*atr; lo=hl2-mult*atr
    fu=np.zeros(len(up)); fl=np.zeros(len(lo)); tr_=np.zeros(len(c),int)
    for i in range(period,len(c)):
        fu[i]=up[i] if (up[i]<fu[i-1] or c[i-1]>fu[i-1]) else fu[i-1]
        fl[i]=lo[i] if (lo[i]>fl[i-1] or c[i-1]<fl[i-1]) else fl[i-1]
        if c[i]>fu[i-1]: tr_[i]=1
        elif c[i]<fl[i-1]: tr_[i]=-1
        else: tr_[i]=tr_[i-1] if i>period else -1
    return tr_[-1]
def alt_index_hourly(files_or_d):
    """median-across-coins hourly log returns -> index OHLC."""
    rows=[]
    if isinstance(files_or_d, dict):
        d=files_or_d
        for k in range(len(d["syms"])):
            a,b=d["offs"][k],d["offs"][k+1]
            rows.append(pd.DataFrame({"ts":d["ts"][a:b],"h":d["h"][a:b],"l":d["l"][a:b],"c":d["c"][a:b],"k":k}))
    else:
        for k,f in enumerate(files_or_d):
            z=np.load(f); rows.append(pd.DataFrame({"ts":z["ts"],"h":z["h"],"l":z["l"],"c":z["c"],"k":k}))
    D=pd.concat(rows); D["hr"]=D.ts//3600*3600
    g=D.groupby(["k","hr"]).agg(h=("h","max"),l=("l","min"),c=("c","last")).reset_index().sort_values(["k","hr"])
    g["pc"]=g.groupby("k").c.shift(1)
    g=g.dropna()
    g["rh"]=np.log(g.h/g.pc); g["rl"]=np.log(g.l/g.pc); g["rc"]=np.log(g.c/g.pc)
    m=g.groupby("hr")[["rh","rl","rc"]].median()
    m=m[g.groupby("hr").size()>=50]
    lc=m.rc.cumsum(); prev=lc.shift(1).fillna(0)
    idx=pd.DataFrame({"c":np.exp(lc),"h":np.exp(prev+m.rh),"l":np.exp(prev+m.rl)},index=m.index)
    return idx
def regimes(idx, btc_ts, btc_c, step=1800, drop=1.5):
    """snapshot every 30 min: ST on last 30 completed hourly bars, BTC 1h change."""
    out=[]
    hrs=idx.index.to_numpy()
    for t in range(int(hrs[0])+30*3600, int(hrs[-1])+3600, step):
        sel=idx[(idx.index<=t-3600)].iloc[-30:]
        if len(sel)<11: continue
        st=supertrend_last(sel.h.to_numpy(),sel.l.to_numpy(),sel.c.to_numpy())
        j=np.searchsorted(btc_ts,t,side="right")-1; j0=np.searchsorted(btc_ts,t-3600,side="right")-1
        bch=(btc_c[j]/btc_c[j0]-1)*100 if j0>=0 else 0
        green=st==1; bear=bch<-drop
        reg="risk_off" if (bear and not green) else ("cautious" if (bear or not green) else "risk_on")
        out.append((t,reg,"green" if green else "red"))
    return pd.DataFrame(out,columns=["ts","regime","st"])
