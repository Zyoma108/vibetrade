"""
Свип Circuit Breaker: что он на самом деле даёт и что стоит.

Вопрос (08.09.2026): CB — защита от просадки или тормоз? Аудит живой БД показал,
что после 2 убытков подряд мат. ожидание остаётся положительным (+0.028R против
+0.095R базовых), то есть кластеризации убытков в данных нет, а reduce стоил
$2.06 за 12 дней. Здесь механизм разбирается на две независимые половины:

  * `circuit_breaker_loss_streak_reduce` — уменьшение размера позиции;
  * `circuit_breaker_loss_streak_stop`   — полная остановка торговли.

Сравнивать только сумму R нечестно: CB — устройство контроля просадки, а не
источник эджа. Поэтому считаем ещё максимальную просадку эквити и худшую серию
убытков — если CB их не уменьшает, у него нет и защитной функции.

⚠️ ВНИМАНИЕ к прошлым результатам этого свипа (до 22.09.2026). Он нормировал
PnL на КОНСТАНТНЫЙ риск $10, хотя весь смысл reduce-режима в том, что он вдвое
урезает бюджет риска. Сделки половинного размера засчитывались с полным весом,
то есть свип, измерявший Circuit Breaker, систематически завышал варианты с
частым reduce. Теперь R берётся из движка, где у каждой сделки свой risk.

⚠️ Свип имеет смысл только на движке с фиксом parity Circuit Breaker
(08.09.2026): до него движок считал убытком любой выход по стопу, включая
прибыльный б/у-выход после партиала, и срабатываний CB было вдвое больше
боевого. См. docs/backtest.md.

Движок цикла симуляции НЕ дублируется: зовём `src.backtest.engine.simulate`.

Использование:
    .venv/bin/python scripts/sweep_circuit_breaker.py --db data/trading_bot.db \
        --out-dir /tmp/sweep_cb
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.backtest.metrics import SIGNIFICANCE_LEGEND, fmt, format_delta  # noqa: E402
from src.config import Settings  # noqa: E402

NEVER = 999  # порог, до которого серия убытков заведомо не дойдёт

# (метка, enabled, reduce, stop, mult%)
VARIANTS = [
    ("off",          False, 2,     3,     50.0),
    ("прод 2/3/50%", True,  2,     3,     50.0),
    ("только reduce", True, 2,     NEVER, 50.0),
    ("только stop",   True, NEVER, 3,     50.0),
    ("мягкий 3/4/70%", True, 3,    4,     70.0),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--has-oi", type=int, default=1)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    log(f"Loading {args.db} ...")
    data = load_data(args.db)
    log(f"Data loaded in {time.time() - t0:.1f}s; {len(VARIANTS)} configs")

    rows = []
    runs: dict[str, list[dict]] = {}
    for label, enabled, reduce_n, stop_n, mult in VARIANTS:
        t1 = time.time()
        s = Settings.from_yaml(args.config)
        s.trading.circuit_breaker_enabled = enabled
        s.trading.circuit_breaker_loss_streak_reduce = reduce_n
        s.trading.circuit_breaker_loss_streak_stop = stop_n
        s.trading.circuit_breaker_reduce_mult_pct = mult
        r = simulate(s, data, has_oi=bool(args.has_oi), collect_retracement=False)
        # R берём из движка: там у каждой сделки собственный risk, поэтому
        # половинный размер в reduce-режиме весит ровно столько, сколько стоит.
        runs[label] = r["trades_list"]
        row = {
            "label": label, "enabled": enabled, "reduce": reduce_n,
            "stop": stop_n, "mult_pct": mult,
            "trades": r["trades"], "win_rate": r["win_rate"],
            "total_R": r["total_R"], "R_per_trade": r["expectancy_R"],
            "R_ci": r["expectancy_R_ci"],
            "max_drawdown_R": r["max_drawdown_R"],
            "worst_loss_streak": r["worst_loss_streak"],
        }
        rows.append(row)
        log(f"{label:16s}: сделок={r['trades']:3d} WR={r['win_rate']:5.1f}% "
            f"R={fmt(r['total_R']):>7} R/сделку={fmt(r['expectancy_R'], '+.4f')} "
            f"макс.просадка={r['max_drawdown_R']:.2f}R "
            f"худшая серия={r['worst_loss_streak']} ({time.time() - t1:.0f}s)")
        with open(out_dir / f"cb_{label.split()[0]}_{reduce_n}_{stop_n}_{mult:g}.json", "w") as f:
            json.dump(r, f, indent=2, default=str)

    log("")
    log(f"{'вариант':17} {'сделок':>7} {'сумма R':>9} {'R/сделку':>10} "
        f"{'просадка':>9} {'серия':>6} {'Δ R к off (95% ДИ)':>28}")
    for r in rows:
        delta = "—"
        if r["label"] != "off" and "off" in runs:
            # Парно: общие сделки дают ровно ноль и не раздувают интервал.
            delta, r["vs_off"] = format_delta(runs[r["label"]], runs["off"])
        log(f"{r['label']:17} {r['trades']:>7} {fmt(r['total_R']):>9} "
            f"{fmt(r['R_per_trade'], '+.4f'):>10} {r['max_drawdown_R']:>8.2f}R "
            f"{r['worst_loss_streak']:>6} {delta:>28}")
    log("")
    log(SIGNIFICANCE_LEGEND)

    # Дамп после таблицы: к этому моменту у строк есть vs_off.
    with open(out_dir / "summary.json", "w") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    log(f"TOTAL elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
