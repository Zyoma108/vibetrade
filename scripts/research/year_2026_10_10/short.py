import numpy as np, numba as nb, pandas as pd
@nb.njit(cache=True)
def sim_short(ts, h, l, c, g0, gend, entry, sl_pct, rr, trig_pct, hold_s, fee_rt):
    sl = entry*(1+sl_pct/100); tp = entry*(1-sl_pct*rr/100)
    trig = entry*(1 - sl_pct*rr*trig_pct/100/100)
    be = False; t0 = ts[g0]
    for j in range(g0+1, gend):
        if trig_pct > 0 and not be and l[j] <= trig:
            be = True; sl = min(sl, entry); continue
        if l[j] <= tp: return 0, (entry-tp)/entry*100/sl_pct - fee_rt/sl_pct
        if h[j] >= sl: return (1 if not be else 2), (entry-sl)/entry*100/sl_pct - fee_rt/sl_pct
        if ts[j]-t0 >= hold_s: return 3, (entry-c[j])/entry*100/sl_pct - fee_rt/sl_pct
    return 3, (entry-c[gend-1])/entry*100/sl_pct - fee_rt/sl_pct
@nb.njit(cache=True)
def many(ts,h,l,c,gs,gends,ents,sl_pct,rr,trig,hold_s,fee_rt):
    n=len(gs); k=np.empty(n,np.int8); R=np.empty(n)
    for i in range(n):
        a,b = sim_short(ts,h,l,c,gs[i],gends[i],ents[i],sl_pct,rr,trig,hold_s,fee_rt); k[i]=a; R[i]=b
    return k,R
