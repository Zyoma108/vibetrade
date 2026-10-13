"""Parity ATR-стопа по правилу 3 AGENTS.md: посделочное сравнение до/после.

Запускается ОДИНАКОВО в двух чекаутах — ветке с правкой и worktree ветки-до
(`git worktree add /tmp/vt_old main`):

    cd <checkout> && PYTHONPATH=<checkout> <repo>/.venv/bin/python \\
        scripts/parity_atr_stop.py <db> <out.json>

Конфиг — config/config.yaml своего чекаута (на ветке ATR-ключи есть, но выключены;
на main их нет — дефолты совпадают, эффективные параметры торговли идентичны).

На стороне ветки дополнительно считается прогон с stop_loss_atr_mult=15
(тайна была в том, какой ATR у реальных сделок): stop_pct каждой сделки / 15
даёт распределение меры на входах, по которому выбирается сетка множителей
свипа. В сравнении parity этот прогон не участвует.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402

DB, OUT = sys.argv[1], sys.argv[2]
CONFIG = sys.argv[3] if len(sys.argv) > 3 else "config/config.yaml"

t0 = time.time()
data = load_data(DB)
ts = data["all_timestamps"]
days = (ts[-1] - ts[0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток")

out = {"db": DB, "days": round(days, 2), "git": None}

s = Settings.from_yaml(CONFIG)
r = simulate(s, data, has_oi=True, collect_retracement=False)
out["prod"] = {
    "trades": [
        {k: t[k] for k in
         ("symbol", "entry_time", "exit_time", "entry_price", "exit_price",
          "pnl", "risk", "exit_reason", "sl_price", "tp_price", "stop_pct")
         if k in t}
        for t in r["trades_list"]
    ],
    "signals": r["signals"],
    "summary": {k: r.get(k) for k in
                ("trades", "total_pnl", "win_rate", "full_take_rate",
                 "max_drawdown_R", "total_R")},
}
log(f"prod: сделок={r['trades']}  ΣR={r.get('total_R', 0):+.2f}")

# Прогон с mult=15 — только там, где конфиг знает про ATR-стоп (ветка с правкой).
if getattr(s.trading, "stop_loss_atr_mult", None) is not None:
    s.trading.stop_loss_atr_mult = 15.0
    r15 = simulate(s, data, has_oi=True, collect_retracement=False)
    out["atr15"] = {
        "trades": [
            {k: t[k] for k in ("symbol", "entry_time", "pnl", "risk",
                               "exit_reason", "stop_pct")}
            for t in r15["trades_list"]
        ],
        "signals": r15["signals"],
    }
    log(f"atr15: сделок={r15['trades']}  ΣR={r15.get('total_R', 0):+.2f}")

json.dump(out, open(OUT, "w"), ensure_ascii=False)
log(f"готово за {time.time()-t0:.0f}s -> {OUT}")
