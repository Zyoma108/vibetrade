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
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402

VIRTUAL_BALANCE = 1000.0
NEVER = 999  # порог, до которого серия убытков заведомо не дойдёт

# (метка, enabled, reduce, stop, mult%)
VARIANTS = [
    ("off",          False, 2,     3,     50.0),
    ("прод 2/3/50%", True,  2,     3,     50.0),
    ("только reduce", True, 2,     NEVER, 50.0),
    ("только stop",   True, NEVER, 3,     50.0),
    ("мягкий 3/4/70%", True, 3,    4,     70.0),
]


def equity_metrics(trades: list[dict], risk: float) -> dict:
    """Просадка и серии по эквити, упорядоченной временем ВЫХОДА из сделки."""
    ordered = sorted(trades, key=lambda t: t["exit_time"] or "")
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    worst_streak = 0
    for t in ordered:
        eq += t["pnl"] / risk
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
        if t["pnl"] <= 0:
            streak += 1
            worst_streak = max(worst_streak, streak)
        else:
            streak = 0
    return {
        "max_drawdown_R": round(max_dd, 2),
        "worst_loss_streak": worst_streak,
        "final_R": round(eq, 2),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--has-oi", type=int, default=1)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    risk = VIRTUAL_BALANCE * (Settings.from_yaml(args.config).trading.risk_per_trade_pct / 100)

    t0 = time.time()
    log(f"Loading {args.db} ...")
    data = load_data(args.db)
    log(f"Data loaded in {time.time() - t0:.1f}s; {len(VARIANTS)} configs")

    rows = []
    for label, enabled, reduce_n, stop_n, mult in VARIANTS:
        t1 = time.time()
        s = Settings.from_yaml(args.config)
        s.trading.circuit_breaker_enabled = enabled
        s.trading.circuit_breaker_loss_streak_reduce = reduce_n
        s.trading.circuit_breaker_loss_streak_stop = stop_n
        s.trading.circuit_breaker_reduce_mult_pct = mult
        r = simulate(s, data, has_oi=bool(args.has_oi), collect_retracement=False)
        m = equity_metrics(r["trades_list"], risk)
        total_r = r["total_pnl"] / risk
        row = {
            "label": label, "enabled": enabled, "reduce": reduce_n,
            "stop": stop_n, "mult_pct": mult,
            "trades": r["trades"], "win_rate": r["win_rate"],
            "total_R": round(total_r, 2),
            "R_per_trade": round(total_r / r["trades"], 4) if r["trades"] else 0.0,
            **m,
        }
        rows.append(row)
        log(f"{label:16s}: сделок={r['trades']:3d} WR={r['win_rate']:5.1f}% "
            f"R={total_r:+7.2f} R/сделку={row['R_per_trade']:+.4f} "
            f"макс.просадка={m['max_drawdown_R']:.2f}R "
            f"худшая серия={m['worst_loss_streak']} ({time.time() - t1:.0f}s)")
        with open(out_dir / f"cb_{label.split()[0]}_{reduce_n}_{stop_n}_{mult:g}.json", "w") as f:
            json.dump(r, f, indent=2, default=str)

    with open(out_dir / "summary.json", "w") as f:
        json.dump(rows, f, indent=2)

    base = next((r for r in rows if r["label"] == "off"), None)
    log("")
    log(f"{'вариант':17} {'сделок':>7} {'сумма R':>9} {'R/сделку':>10} "
        f"{'просадка':>9} {'серия':>6} {'Δ R к off':>10}")
    for r in rows:
        d = f"{r['total_R'] - base['total_R']:+.2f}" if base else "—"
        log(f"{r['label']:17} {r['trades']:>7} {r['total_R']:>+9.2f} "
            f"{r['R_per_trade']:>+10.4f} {r['max_drawdown_R']:>8.2f}R "
            f"{r['worst_loss_streak']:>6} {d:>10}")
    log(f"TOTAL elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
