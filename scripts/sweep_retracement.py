"""
Свип порога `max_window_retracement_pct` на реальной БД.

Движок симуляции — `src/backtest/engine.py` (раньше жил прямо здесь; см. его
docstring про то, почему реализаций было три). Здесь остался только CLI свипа.

Использование:
    .venv/bin/python scripts/sweep_retracement.py --db data/trading_bot.db \
        --thresholds 0,1.0,1.5,2.0,3.0,5.0 --out-dir /path/to/out

Пилотный прогон (проверка корректности/скорости перед полным свипом):
    .venv/bin/python scripts/sweep_retracement.py --limit-days 3 --thresholds 0,2.0
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--thresholds", default="0,1.0,1.5,2.0,3.0,5.0")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit-days", type=float, default=None)
    ap.add_argument("--limit-symbols", default=None, help="comma-separated")
    ap.add_argument("--has-oi", type=int, default=1)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    limit_symbols = args.limit_symbols.split(",") if args.limit_symbols else None
    thresholds = [float(x) for x in args.thresholds.split(",")]

    t0 = time.time()
    log(f"Loading data from {args.db} (limit_days={args.limit_days}, limit_symbols={limit_symbols}) ...")
    data = load_data(args.db, limit_days=args.limit_days, limit_symbols=limit_symbols)
    log(f"Data loaded in {time.time()-t0:.1f}s")

    for th in thresholds:
        t1 = time.time()
        settings = Settings.from_yaml(args.config)
        settings.strategy.max_window_retracement_pct = th
        log(f"Running simulation for max_window_retracement_pct={th} ...")
        result = simulate(settings, data, has_oi=bool(args.has_oi))
        elapsed = time.time() - t1
        log(
            f"th={th}: signals={result['signals']} trades={result['trades']} "
            f"win_rate={result['win_rate']}% total_pnl={result['total_pnl']} "
            f"retracement_blocked={result['price_stage_counts'].get('retracement', 0)} "
            f"shift_used={result['shift_used_count']} "
            f"({elapsed:.1f}s)"
        )
        out_path = out_dir / f"retracement_{th}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        log(f"Saved {out_path}")

    log(f"TOTAL elapsed: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
