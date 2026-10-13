"""Распределение ATR(14) в точках входа боевых сделок — для сетки множителей свипа.

    .venv/bin/python scripts/measure_atr_at_entries.py <db> <parity_json>

parity_json — вывод scripts/parity_atr_stop.py (список сделок prod). Для каждой
сделки считается ATR(14) обоими источниками (baseline — бары до sustain-окна,
signal — включая его) на том же ряду свечей, что видит детектор. Печатаются
перцентили и множители, дающие стоп 2/3/4%.
"""
import json
import sqlite3
import sys
from collections import defaultdict

import numpy as np

DB, PARITY = sys.argv[1], sys.argv[2]
PERIOD, SUSTAIN = 14, 4

trades = json.load(open(PARITY))["prod"]["trades"]

con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
rows = con.execute(
    "SELECT symbol, timestamp, high, low, close FROM candles "
    "WHERE volume > 0 ORDER BY symbol, timestamp"
).fetchall()
con.close()

def _norm(ts: str) -> str:
    return ts[:19].replace("T", " ")

series = {}
by_sym: dict[str, list[tuple]] = defaultdict(list)
for sym, ts, h, l, c in rows:
    by_sym[sym].append((ts, h, l, c))
for sym, lst in by_sym.items():
    arr = np.array([(h, l, c) for _, h, l, c in lst], dtype=float)
    ts_index = {_norm(t): i for i, (t, *_ ) in enumerate(lst)}
    h, l, c = arr[:, 0], arr[:, 1], arr[:, 2]
    prev_c = np.roll(c, 1)
    tr = np.maximum.reduce([h - l, np.abs(h - prev_c), np.abs(l - prev_c)])
    tr[0] = tr[1] if len(tr) > 1 else 0.0
    series[sym] = (tr, c, ts_index)

def atr_at(sym, ts, source):
    tr, closes, idx = series.get(sym, (None, None, None))
    if tr is None or ts not in idx:
        return None
    i = idx[ts]
    end = i + 1 if source == "signal" else i - SUSTAIN + 1
    start = end - PERIOD
    if start < 1:
        return None
    return float(np.mean(tr[start:end]) / closes[i] * 100)

vals = {"baseline": [], "signal": []}
missed = 0
for t in trades:
    ts = _norm(t["entry_time"])
    got = False
    for src in vals:
        v = atr_at(t["symbol"], ts, src)
        if v is not None:
            vals[src].append(v)
            got = True
    if not got:
        missed += 1

for src, v in vals.items():
    a = np.array(v)
    if not len(a):
        print(f"{src}: нет данных")
        continue
    q = np.percentile(a, [10, 25, 50, 75, 90])
    print(f"\n{src}: n={len(a)} (не найдено баров: {missed})")
    print("  ATR %:  p10={:.3f}  p25={:.3f}  med={:.3f}  p75={:.3f}  p90={:.3f}".format(*q))
    for target in (2.0, 3.0, 4.0):
        print(f"  стоп {target}% ← mult ≈ {target / np.median(a):.1f} (по медиане), "
              f"{target / np.percentile(a, 75):.1f} (по p75)")
