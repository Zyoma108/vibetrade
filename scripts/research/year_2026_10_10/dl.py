import io, zipfile, urllib.request, concurrent.futures as cf, numpy as np, os, sys, time
OUT = "../hist"
z = [np.load(f"../dat/{p}_candles.npz", allow_pickle=True)["syms"] for p in ["P1","P2","P3","P4"]]
syms = sorted(set(s.split("/")[0] + "USDT" for zz in z for s in zz))
months = [f"2025-{m:02d}" for m in range(10,13)] + [f"2026-{m:02d}" for m in range(1,10)]
def fetch(sym):
    path = f"{OUT}/{sym}.npz"
    if os.path.exists(path): return sym, "cached"
    arrs = []
    for mo in months:
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{sym}/3m/{sym}-3m-{mo}.zip"
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    b = r.read()
                break
            except urllib.error.HTTPError as e:
                b = None
                if e.code == 404: break
                time.sleep(2)
            except Exception:
                b = None; time.sleep(2)
        if not b: continue
        import pandas as pd
        with zipfile.ZipFile(io.BytesIO(b)) as zf:
            raw = zf.read(zf.namelist()[0])
        hdr = raw[:9] == b"open_time"
        df = pd.read_csv(io.BytesIO(raw), header=0 if hdr else None, usecols=list(range(11)))
        arrs.append(df.to_numpy(dtype=float))
    if not arrs: return sym, "none"
    A = np.concatenate(arrs)
    np.savez(path, ts=(A[:,0]//1000).astype(np.int64), o=A[:,1], h=A[:,2], l=A[:,3], c=A[:,4], v=A[:,5], qv=A[:,7], n=A[:,8], tbv=A[:,9])
    return sym, len(A)
print(len(syms), "symbols", flush=True)
t0=time.time(); done=0
with cf.ThreadPoolExecutor(32) as ex:
    for sym, r in ex.map(fetch, syms):
        done += 1
        if done % 25 == 0: print(done, sym, r, f"{time.time()-t0:.0f}s", flush=True)
print("finished", time.time()-t0)
