import json, os, sys, pickle
sys.path.insert(0, os.getcwd())
from src.backtest.metrics import compare, summarize
SP = sys.argv[1]
rep = pickle.load(open(f"{SP}/hist/sweep3.pkl","rb"))
rep2 = pickle.load(open(f"{SP}/hist/sweep2.pkl","rb"))
import datetime as dt
def key(t): return (t["symbol"], t["entry_time"])
for P in ["P1","P2","P3","P4"]:
    base = json.load(open(f"{SP}/base/{P}.json")); aft = json.load(open(f"{SP}/after/{P}.json")); days = aft["days"]
    print(f"== {P} ({days:.1f} сут)")
    for stage in ["clean_1000","lot_60"]:
        b = base["stages"][stage]["trades"]; d = aft["runs"][f"default/{stage}"]["trades"]
        kb = {key(t): round(t["pnl"]/t["risk"],9) for t in b}; kd = {key(t): round(t["pnl"]/t["risk"],9) for t in d}
        print(f"  [{stage}] rule3 default==main: {kb==kd} ({len(kb)} vs {len(kd)})")
        for v in ["stop4","stop4_trail8_4"]:
            a = aft["runs"][f"{v}/{stage}"]["trades"]
            c = compare(a, d, days=days); s = summarize(a, days=days); s0 = summarize(d, days=days)
            print(f"    {v:15s} n {s0['trades']}->{s['trades']} ΣR {s0['total_R']:+.2f}->{s['total_R']:+.2f}  Δ {c['delta_R']:+.2f} {c['delta_R_ci']}{' *' if c['significant'] else ''}")
            if stage == "clean_1000":
                lbl = "s4_a8_d4" if "trail" in v else "stop4"
                src = rep if "trail" in v else rep2
                rk = src[P][lbl]["keys"]
                ek = {f"{t['symbol']}|{int(dt.datetime.fromisoformat(t['entry_time']).replace(tzinfo=dt.timezone.utc).timestamp())}": t["pnl"]/t["risk"] for t in a}
                same = set(rk)==set(ek); mx = max(abs(rk[k]-ek[k]) for k in set(rk)&set(ek))
                print(f"      replica==engine: trades {same} ({len(rk)} vs {len(ek)}), max|dR| {mx:.1e}")
