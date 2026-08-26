"""
Свип trading.risk_reward_ratio (сдвигает TP дальше при фиксированном SL=5%) на
реальной БД, поверх текущего прод-конфига (partial_close_pct=35%,
partial_close_qty_pct=30%, retracement-фильтр 2.0 — всё как есть). Контекст:
проверяем, не выгоднее ли отодвинуть TP дальше, раз сделки, доходящие до
текущего TP=10%, в среднем продолжают расти ещё (см. analyze_tp_upside.py) —
но раздвигание TP также рискует конвертировать часть НЫНЕШНИХ tp-сделок в
partial_be_stop/sl, если цена не дойдёт до нового, более далёкого таргета.

Использование:
    .venv/bin/python scripts/sweep_rr.py --db data/trading_bot.db \
        --thresholds 2.0,2.5,3.0,3.5,4.0 --out-dir /path/to/out
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402

OUTCOME_LABELS = {
    "full_sl": "Полный SL (партиал не сработал)",
    "full_timeout": "Time/EOD-выход без партиала",
    "full_tp_no_partial": "TP без партиала (аномалия/edge case)",
    "partial_be_stop": "Партиал + остаток закрыт по б/у-стопу (грайнд)",
    "partial_timeout": "Партиал + остаток закрыт по времени/EOD",
    "partial_tp": "Партиал + остаток дошёл до полного TP",
    "other": "Прочее",
}


def categorize(t: dict) -> str:
    pc = t["partial_closed"]
    er = t["exit_reason"]
    if not pc and er == "sl":
        return "full_sl"
    if not pc and er in ("time", "eod"):
        return "full_timeout"
    if not pc and er == "tp":
        return "full_tp_no_partial"
    if pc and er == "sl":
        return "partial_be_stop"
    if pc and er in ("time", "eod"):
        return "partial_timeout"
    if pc and er == "tp":
        return "partial_tp"
    return "other"


def summarize_outcomes(trades: list[dict]) -> dict:
    cats: dict[str, dict] = {}
    for t in trades:
        cat = categorize(t)
        c = cats.setdefault(cat, {"count": 0, "pnl": 0.0})
        c["count"] += 1
        c["pnl"] += t["pnl"]
    for c in cats.values():
        c["avg_pnl"] = round(c["pnl"] / c["count"], 2) if c["count"] else 0.0
        c["pnl"] = round(c["pnl"], 2)
    return cats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--thresholds", default="2.0,2.5,3.0,3.5,4.0")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--has-oi", type=int, default=1)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    thresholds = [float(x) for x in args.thresholds.split(",")]

    t0 = time.time()
    log(f"Loading data from {args.db} ...")
    data = load_data(args.db)
    log(f"Data loaded in {time.time()-t0:.1f}s")

    summary_rows = []
    for th in thresholds:
        t1 = time.time()
        settings = Settings.from_yaml(args.config)
        settings.trading.risk_reward_ratio = th
        tp_pct = settings.trading.stop_loss_pct * th
        log(f"Running simulation for risk_reward_ratio={th} (TP={tp_pct:.1f}%, SL={settings.trading.stop_loss_pct}%) ...")
        result = simulate(settings, data, has_oi=bool(args.has_oi), collect_retracement=False)
        elapsed = time.time() - t1

        outcomes = summarize_outcomes(result["trades_list"])
        log(
            f"RR={th} (TP={tp_pct:.1f}%): signals={result['signals']} trades={result['trades']} "
            f"win_rate={result['win_rate']}% total_pnl={result['total_pnl']} "
            f"avg_pnl={result['avg_pnl']} ({elapsed:.1f}s)"
        )
        for cat, label in OUTCOME_LABELS.items():
            if cat in outcomes:
                o = outcomes[cat]
                log(f"    {label}: n={o['count']} pnl={o['pnl']} avg={o['avg_pnl']}")

        result["outcomes"] = outcomes
        out_path = out_dir / f"rr_{th}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)

        summary_rows.append({
            "rr": th,
            "tp_pct": round(tp_pct, 2),
            "trades": result["trades"],
            "win_rate": result["win_rate"],
            "total_pnl": result["total_pnl"],
            "avg_pnl": result["avg_pnl"],
            "total_fees": result["total_fees"],
            "outcomes": outcomes,
        })

    with open(out_dir / "summary.json", "w") as f:
        json.dump(summary_rows, f, indent=2)
    log(f"Saved {out_dir / 'summary.json'}")
    log(f"TOTAL elapsed: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
