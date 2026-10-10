import sqlite3, sys, time, numpy as np, pandas as pd
DBS = {
 "P1": "/Users/rsentsov/Develop/vibetrade/data/trading_bot_10.08-25.08.db",
 "P2": "/Users/rsentsov/Develop/vibetrade/data/trading_bot_27.08-22.09.db",
 "P3": "/Users/rsentsov/Develop/vibetrade/data/trading_bot_22.09-28.09.db",
 "P4": "/Users/rsentsov/Develop/vibetrade/data/trading_bot_28.09-10.10.db",
}
OUT = sys.argv[1]
for name in sys.argv[2:]:
    t0 = time.time()
    db = sqlite3.connect(f"file:{DBS[name]}?immutable=1", uri=True)
    df = pd.read_sql_query("SELECT symbol, exchange, timestamp, open, high, low, close, volume FROM candles WHERE volume > 0", db)
    df["ts"] = pd.to_datetime(df["timestamp"], format="mixed").astype("datetime64[s]").astype("int64")
    df = df.drop(columns="timestamp").sort_values(["symbol", "exchange", "ts"], kind="stable")
    # engine: keep the (exchange, symbol) series with more bars; tie -> first encountered in ORDER BY symbol, exchange
    cnt = df.groupby(["symbol", "exchange"], sort=True).size().reset_index(name="n")
    keep = []
    for sym, g in cnt.groupby("symbol", sort=True):
        best = None
        for _, r in g.iterrows():
            if best is None or r.n > best.n:
                best = r
        keep.append((sym, best.exchange))
    keep = pd.DataFrame(keep, columns=["symbol", "exchange"])
    df = df.merge(keep, on=["symbol", "exchange"]).sort_values(["symbol", "ts"], kind="stable")
    syms = df["symbol"].unique()
    sizes = df.groupby("symbol", sort=False).size().reindex(syms).to_numpy()
    offs = np.concatenate([[0], np.cumsum(sizes)])
    np.savez(f"{OUT}/{name}_candles.npz", syms=np.array(syms, dtype=object), exch=keep.set_index("symbol").loc[syms, "exchange"].to_numpy().astype(object),
             offs=offs, ts=df["ts"].to_numpy(np.int64), o=df["open"].to_numpy(float), h=df["high"].to_numpy(float),
             l=df["low"].to_numpy(float), c=df["close"].to_numpy(float), v=df["volume"].to_numpy(float))
    mc = pd.read_sql_query("SELECT timestamp, regime, supertrend_color FROM market_context_snapshots ORDER BY timestamp", db)
    mc["ts"] = pd.to_datetime(mc["timestamp"], format="mixed").astype("datetime64[s]").astype("int64")
    mc.to_parquet(f"{OUT}/{name}_mc.parquet")
    for t in ["trades", "signals", "filtered_signals"]:
        x = pd.read_sql_query(f"SELECT * FROM {t}", db)
        x.to_parquet(f"{OUT}/{name}_{t}.parquet")
    print(name, len(df), len(syms), f"{time.time()-t0:.0f}s", flush=True)
