"""
Свип параметра trading.partial_close_pct на реальной БД (data/trading_bot.db),
ПОВЕРХ live-эксперимента strategy.max_window_retracement_pct=2.0 (уже в проде
с 10.08.2026 — см. память retracement-filter-live-experiment). Базовый конфиг
для всех прогонов — актуальный config/config.yaml как есть, меняется только
trading.partial_close_pct.

Полностью переиспользует движок из scripts/sweep_retracement.py (load_data,
_regime_at, compute_retracement_pct, simulate) — симуляция портфеля идентична
(TP/SL/partial/circuit-breaker/market-context/cooldown/pending-entry), меняется
только то, какой параметр варьируется между прогонами.

Дополнительно (--baseline-bucketing): один отдельный прогон с ОТКЛЮЧЁННЫМ
partial-триггером (partial_close_pct=1000%, физически недостижимо → SL никогда
не переносится в безубыток, сделки идут "сырым" путём до реального SL/TP/time).
Для каждой закрытой в этом прогоне сделки пост-хок считается max_progress_pct —
максимальная доля пути от входа к TP, которой достигла цена (high) за время
жизни сделки, независимо от исхода. Затем бакетируется по порогам свипа — это
даёт ощущение чувствительности БЕЗ N отдельных прогонов, но НЕ заменяет
основной результат (независимые прогоны с обратной связью через circuit-breaker/
cooldown/max_positions, т.к. этот "сырой" прогон открывает/держит немного другой
набор сделок из-за отсутствия партиал-переноса SL).

Использование:
    .venv/bin/python scripts/sweep_partial_close.py --db data/trading_bot.db \
        --thresholds 10,15,20,25,30,35,40,50 --out-dir /path/to/out

Пилотный прогон:
    .venv/bin/python scripts/sweep_partial_close.py --limit-days 3 --thresholds 20,35 \
        --out-dir /tmp/pilot
"""

import argparse
import json
import sys
import time
from datetime import datetime
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


def compute_max_progress_pct(data: dict, trade: dict) -> float | None:
    """Пост-хок: максимальная % пути от entry к tp_price, достигнутая ценой (high)
    за время жизни сделки [entry_time, exit_time]. Использует свечи из уже
    загруженного data['symbols'] (без повторного похода в БД)."""
    sym = trade["symbol"]
    sym_rows = data["symbols"].get(sym)
    if not sym_rows:
        return None
    entry_price = trade["entry_price"]
    tp_price = trade["tp_price"]
    denom = tp_price - entry_price
    if denom <= 0:
        return None
    entry_t = datetime.fromisoformat(trade["entry_time"])
    exit_t = datetime.fromisoformat(trade["exit_time"]) if trade["exit_time"] else None
    max_high = entry_price
    for ts, o, h, l, c, v in sym_rows:
        if ts < entry_t:
            continue
        if exit_t is not None and ts > exit_t:
            break
        if h > max_high:
            max_high = h
    return (max_high - entry_price) / denom * 100


def bucket_progress(data: dict, trades: list[dict], thresholds: list[float]) -> dict:
    progresses = []
    for t in trades:
        p = compute_max_progress_pct(data, t)
        if p is not None:
            progresses.append(p)
    n = len(progresses)
    buckets = {}
    for th in thresholds:
        reached = sum(1 for p in progresses if p >= th)
        buckets[th] = {
            "reached": reached,
            "total": n,
            "pct": round(100.0 * reached / n, 1) if n else 0.0,
        }
    return {"n_trades_with_progress": n, "buckets": buckets, "raw": progresses}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--thresholds", default="10,15,20,25,30,35,40,50")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit-days", type=float, default=None)
    ap.add_argument("--limit-symbols", default=None, help="comma-separated")
    ap.add_argument("--has-oi", type=int, default=1)
    ap.add_argument("--baseline-bucketing", type=int, default=1,
                     help="1=запустить доп. прогон с отключённым партиалом для bucketing (по умолчанию вкл)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    limit_symbols = args.limit_symbols.split(",") if args.limit_symbols else None
    thresholds = [float(x) for x in args.thresholds.split(",")]

    t0 = time.time()
    log(f"Loading data from {args.db} (limit_days={args.limit_days}, limit_symbols={limit_symbols}) ...")
    data = load_data(args.db, limit_days=args.limit_days, limit_symbols=limit_symbols)
    log(f"Data loaded in {time.time()-t0:.1f}s")

    # -------------------------------------------------------------------
    # Baseline bucketing run: partial трigger отключён (порог 1000% —
    # физически недостижим), max_window_retracement_pct берётся как есть
    # из config.yaml (текущий прод, live-эксперимент 2.0)
    # -------------------------------------------------------------------
    if args.baseline_bucketing:
        t1 = time.time()
        settings = Settings.from_yaml(args.config)
        settings.trading.partial_close_pct = 1000.0
        log("Running baseline bucketing simulation (partial disabled, retracement filter as in prod config) ...")
        base_result = simulate(settings, data, has_oi=bool(args.has_oi), collect_retracement=False)
        elapsed = time.time() - t1
        log(
            f"baseline(no-partial): signals={base_result['signals']} trades={base_result['trades']} "
            f"win_rate={base_result['win_rate']}% total_pnl={base_result['total_pnl']} ({elapsed:.1f}s)"
        )
        bucketing = bucket_progress(data, base_result["trades_list"], thresholds)
        log(f"TP-progress bucketing (n={bucketing['n_trades_with_progress']}):")
        for th in thresholds:
            b = bucketing["buckets"][th]
            log(f"  >= {th}% path to TP: {b['reached']}/{b['total']} ({b['pct']}%)")
        with open(out_dir / "baseline_bucketing.json", "w") as f:
            json.dump({"result": base_result, "bucketing": bucketing}, f, indent=2)
        log(f"Saved {out_dir / 'baseline_bucketing.json'}")

    # -------------------------------------------------------------------
    # Full independent portfolio sims per partial_close_pct threshold
    # -------------------------------------------------------------------
    summary_rows = []
    for th in thresholds:
        t1 = time.time()
        settings = Settings.from_yaml(args.config)
        settings.trading.partial_close_pct = th
        log(f"Running simulation for partial_close_pct={th} ...")
        result = simulate(settings, data, has_oi=bool(args.has_oi), collect_retracement=False)
        elapsed = time.time() - t1

        outcomes = summarize_outcomes(result["trades_list"])
        log(
            f"th={th}%: signals={result['signals']} trades={result['trades']} "
            f"win_rate={result['win_rate']}% total_pnl={result['total_pnl']} "
            f"avg_pnl={result['avg_pnl']} ({elapsed:.1f}s)"
        )
        for cat, label in OUTCOME_LABELS.items():
            if cat in outcomes:
                o = outcomes[cat]
                log(f"    {label}: n={o['count']} pnl={o['pnl']} avg={o['avg_pnl']}")

        result["outcomes"] = outcomes
        out_path = out_dir / f"partial_{th}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        log(f"Saved {out_path}")

        summary_rows.append({
            "threshold": th,
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
