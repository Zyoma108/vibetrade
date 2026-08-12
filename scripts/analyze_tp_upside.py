"""
Для сделок, реально дошедших до полного TP (exit_reason == "tp") в бэктесте —
сколько ДОПОЛНИТЕЛЬНОГО движения было упущено после того, как хардовый TP-ордер
их закрыл. Отвечает на вопрос: если бы вместо жёсткого TP была лесенка/трейлинг,
сколько ещё роста можно было бы поймать?

Для каждой такой сделки берёт свечи символа с момента TP-хита (exit_time) до
entry_time + max_hold_hours (дедлайн удержания по конфигу) и ищет максимальный
high в этом окне — это "потолок", которого движение реально достигло. Также
считает giveback: куда цена откатилась к концу окна относительно пика (насколько
реалистично было бы поймать этот пик трейлингом/второй лесенкой, а не он исчезает
за одну свечу).

Использование:
    .venv/bin/python scripts/analyze_tp_upside.py --db data/trading_bot.db
"""

import argparse
import json
import sys
import time
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sweep_retracement import load_data, simulate, log  # noqa: E402
from src.config import Settings  # noqa: E402


def analyze(settings, data, has_oi: bool = True):
    result = simulate(settings, data, has_oi=has_oi, collect_retracement=False)
    tp_trades = [t for t in result["trades_list"] if t["exit_reason"] == "tp"]
    log(f"Всего сделок: {result['trades']}, дошли до полного TP: {len(tp_trades)}")

    max_hold_hours = settings.trading.max_hold_hours
    rows_out = []
    for t in tp_trades:
        sym = t["symbol"]
        sym_rows = data["symbols"].get(sym)
        if not sym_rows:
            continue
        entry_price = t["entry_price"]
        tp_price = t["tp_price"]
        from datetime import datetime as _dt
        entry_t = _dt.fromisoformat(t["entry_time"])
        exit_t = _dt.fromisoformat(t["exit_time"])
        deadline = entry_t + timedelta(hours=max_hold_hours)

        peak_high = tp_price
        peak_ts = exit_t
        last_close_at_deadline = None
        for ts, o, h, l, c, v in sym_rows:
            if ts < exit_t:
                continue
            if ts > deadline:
                break
            if h > peak_high:
                peak_high = h
                peak_ts = ts
            last_close_at_deadline = c

        extra_pct_beyond_tp = (peak_high / tp_price - 1) * 100
        extra_pct_beyond_entry_total = (peak_high / entry_price - 1) * 100
        tp_pct_configured = (tp_price / entry_price - 1) * 100
        hours_to_peak = (peak_ts - exit_t).total_seconds() / 3600
        giveback_pct = None
        if last_close_at_deadline is not None and peak_high > 0:
            giveback_pct = (1 - last_close_at_deadline / peak_high) * 100

        rows_out.append({
            "symbol": sym,
            "entry_time": t["entry_time"],
            "tp_hit_time": t["exit_time"],
            "entry_price": entry_price,
            "tp_price": tp_price,
            "tp_pct_configured": round(tp_pct_configured, 2),
            "peak_high_after_tp": peak_high,
            "extra_pct_beyond_tp": round(extra_pct_beyond_tp, 2),
            "extra_pct_beyond_entry_total": round(extra_pct_beyond_entry_total, 2),
            "hours_from_tp_to_peak": round(hours_to_peak, 2),
            "giveback_pct_from_peak_to_deadline": round(giveback_pct, 2) if giveback_pct is not None else None,
        })

    return rows_out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--limit-days", type=float, default=None)
    ap.add_argument("--has-oi", type=int, default=1)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    t0 = time.time()
    log(f"Loading data from {args.db} ...")
    data = load_data(args.db, limit_days=args.limit_days)
    log(f"Data loaded in {time.time()-t0:.1f}s")

    settings = Settings.from_yaml(args.config)
    rows = analyze(settings, data, has_oi=bool(args.has_oi))

    rows.sort(key=lambda r: -r["extra_pct_beyond_tp"])
    log(f"{'symbol':<14} {'tp%':>6} {'extra_vs_tp%':>13} {'extra_vs_entry%':>16} {'h_to_peak':>10} {'giveback%':>10}")
    for r in rows:
        log(
            f"{r['symbol']:<14} {r['tp_pct_configured']:>6} {r['extra_pct_beyond_tp']:>13} "
            f"{r['extra_pct_beyond_entry_total']:>16} {r['hours_from_tp_to_peak']:>10} "
            f"{r['giveback_pct_from_peak_to_deadline']:>10}"
        )

    if rows:
        avg_extra = sum(r["extra_pct_beyond_tp"] for r in rows) / len(rows)
        median_extra = sorted(r["extra_pct_beyond_tp"] for r in rows)[len(rows) // 2]
        log(f"\nСреднее доп. движение сверх TP: {avg_extra:.2f}% | медиана: {median_extra:.2f}%")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(rows, f, indent=2)
        log(f"Saved {args.out}")


if __name__ == "__main__":
    main()
