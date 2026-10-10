"""Engine run with config overrides. Run from repo root of the code being measured."""
import json, os, sqlite3, sys, time
sys.path.insert(0, os.getcwd())
_c = sqlite3.connect
sqlite3.connect = lambda p, *a, **k: _c(f"file:{os.path.abspath(p)}?immutable=1", *a, uri=True, **k) if isinstance(p, str) and not p.startswith("file:") else _c(p, *a, **k)
from src.backtest.engine import load_data, load_markets, log, simulate
from src.config import Settings
db, out_path = sys.argv[1], sys.argv[2]
VARIANTS = {
    "default": {},
    "stop4": {"stop_loss_pct": 4.0},
    "stop4_trail8_4": {"stop_loss_pct": 4.0, "trailing_activation_pct": 8.0, "trailing_distance_pct": 4.0},
}
STAGES = {"clean_1000": (1000.0, None), "lot_60": (60.0, "config/bybit_markets.json")}
data = load_data(db)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
out = {"db": db, "days": days, "runs": {}}
for vname, ov in VARIANTS.items():
    for stage, (dep, mk) in STAGES.items():
        s = Settings.from_yaml("config/config.yaml")
        s.trading.risk_per_trade_pct = 1.0
        s.trading.backtest_deposit_usdt = dep
        for k, v in ov.items(): setattr(s.trading, k, v)
        r = simulate(s, data, has_oi=True, collect_retracement=False, markets=load_markets(mk) if mk else None)
        out["runs"][f"{vname}/{stage}"] = {"trades": r["trades_list"], "total_R": r["total_R"], "full_take_rate": r["full_take_rate"]}
        log(f"{vname}/{stage}: {r['trades']} trades ΣR {r['total_R']:+.2f}")
json.dump(out, open(out_path, "w"), default=str)
