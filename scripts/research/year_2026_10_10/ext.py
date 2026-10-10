"""Extended per-coin features at the decision bar e (window end), research only."""
import numpy as np, pandas as pd


def _lag(x, k):
    r = np.full(len(x), np.nan)
    if k < len(x):
        r[k:] = x[:len(x) - k]
    return r


def ext_features(d, X):
    """Adds x_* columns to X (rows: candidate bars). Decision bar e = i-1 if shifted else i."""
    offs = d["offs"]
    parts = []
    for k, idx in X.groupby("k").groups.items():
        a, b = offs[k], offs[k + 1]
        o, h, l, c, v, ts = (d[x][a:b] for x in ("o", "h", "l", "c", "v", "ts"))
        n = len(c)
        S = lambda x, w, f: getattr(pd.Series(x).rolling(w, min_periods=max(2, w // 2)), f)().to_numpy()
        lr = np.concatenate([[np.nan], np.diff(np.log(c))])
        rng_bar = (h - l) / c * 100
        usd = v * c
        with np.errstate(all="ignore"):
            F = {
                "r1h": (c / _lag(c, 20) - 1) * 100,
                "r4h": (c / _lag(c, 80) - 1) * 100,
                "r12h": (c / _lag(c, 240) - 1) * 100,
                "r24h": (c / _lag(c, 480) - 1) * 100,
                "fromlow24": (c / S(l, 480, "min") - 1) * 100,
                "fromhigh24": (c / S(h, 480, "max") - 1) * 100,
                "fromlow4h": (c / S(l, 80, "min") - 1) * 100,
                "brk_win": (c / _lag(S(h, 80, "max"), 4) - 1) * 100,
                "brk24": (c / _lag(S(h, 476, "max"), 4) - 1) * 100,
                "vol_base": _lag(S(lr, 70, "std"), 14) * 100,
                "atr_base": _lag(S(rng_bar, 70, "mean"), 14),
                "atr_4h": _lag(S(rng_bar, 80, "mean"), 4),
                "usd24": S(usd, 480, "sum"),
                "usd_base_med": _lag(S(usd, 70, "median"), 14),
                "pre_vol": _lag(S(v, 10, "median"), 4),
                "uwick": np.where(h - l > 0, (h - np.maximum(o, c)) / (h - l), np.nan),
                "green4": S((c > o).astype(float), 4, "sum"),
                "maxbar4": S((c / o - 1) * 100, 4, "max"),
                "minbar4": S((c / o - 1) * 100, 4, "min"),
                "lastbar": (c / o - 1) * 100,
                "rng_last": rng_bar,
                "v_last": v,
            }
            # repeat surges: number of bars in previous 24h where 4-bar min volume >= 5x rolling-70 median
            vmin4 = S(v, 4, "min")
            med70 = _lag(pd.Series(v).rolling(70).median().to_numpy(), 14)
            surge_flag = (vmin4 >= 5 * med70).astype(float)
            F["surges24"] = _lag(pd.Series(surge_flag).rolling(476, min_periods=1).sum().to_numpy(), 4)
            F["surges4h"] = _lag(pd.Series(surge_flag).rolling(76, min_periods=1).sum().to_numpy(), 4)
        Fd = pd.DataFrame(F)
        sub = X.loc[idx]
        e = sub["i"].to_numpy() - sub["shifted"].to_numpy().astype(int)
        P = Fd.iloc[e].add_prefix("x_")
        P.index = idx
        P["x_pre_vol"] = P["x_pre_vol"] / sub["u_base_v"].to_numpy()
        P["x_v_last"] = P["x_v_last"] / sub["u_base_v"].to_numpy()
        P["x_hour"] = (sub["ts"].to_numpy() // 3600) % 24
        P["x_stop_atr"] = 5.0 / P["x_atr_base"]
        parts.append(P)
    return X.join(pd.concat(parts))
