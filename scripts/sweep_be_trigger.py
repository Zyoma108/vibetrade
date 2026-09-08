"""
Свип защиты позиции: доля частичной фиксации × точка её срабатывания.

Проверяемая гипотеза (08.09.2026): перевод стопа в безубыток БЕЗ частичной
фиксации (`partial_close_qty_pct = 0`) выгоднее текущих 30%, потому что весь
объём доезжает до полного TP. Арифметика на живой выборке: сделка, дошедшая до
TP, стоит 2.0R вместо 1.61R (+0.39R), сделка, выбитая б/у-стопом, — 0R вместо
+0.21R (−0.21R). Итог зависит от соотношения TP / б/у в популяции, поэтому
считаем движком, а не на бумаге.

Второе измерение — `partial_close_pct` (ЦЕНОВОЙ порог срабатывания, % пути до
TP). Аудит 08.09.2026 показал, что точка включения защиты двигает результат в
разы сильнее доли, поэтому свипать их по отдельности бессмысленно: доля 0%
означает «б/у-стоп на этом уровне», и её оптимум зависит от того, где стоит
уровень.

⚠️ `partial_close_qty_pct = 0` выходит за границу Pydantic (`ge=5.0`).
Присваивание в обход валидации здесь допустимо (модель без
`validate_assignment`), но для прода границу придётся опустить до 0.0 — 0 стал
осмысленным значением, а не «выключено».

Движок цикла симуляции НЕ дублируется: зовём `src.backtest.engine.simulate`,
меняются только два поля конфига между прогонами (см. docs/backtest.md).

Использование:
    .venv/bin/python scripts/sweep_be_trigger.py --db data/trading_bot.db \
        --qty 0,10,20,30,50 --trigger 25,35,50,65 --out-dir /tmp/sweep
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402

# Риск на сделку в движке: virtual_balance 1000 × risk_per_trade_pct.
# Нужен только чтобы переводить $ в R — свипы сравниваются в R.
VIRTUAL_BALANCE = 1000.0


def categorize(t: dict) -> str:
    """Исход сделки в терминах защиты позиции."""
    pc = t["partial_closed"]  # для qty=0 это «б/у-стоп взведён», без фиксации
    er = t["exit_reason"]
    if not pc:
        return "no_protect_sl" if er == "sl" else f"no_protect_{er}"
    if er == "sl":
        return "protect_be_stop"
    if er == "tp":
        return "protect_tp"
    return "protect_timeout"


def summarize(trades: list[dict], risk: float) -> dict:
    cats: dict[str, dict] = {}
    for t in trades:
        c = cats.setdefault(categorize(t), {"n": 0, "R": 0.0})
        c["n"] += 1
        c["R"] += t["pnl"] / risk
    for c in cats.values():
        c["R"] = round(c["R"], 2)
    return cats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--qty", default="0,10,20,30,50",
                    help="доли позиции (%%), закрываемые по триггеру; 0 = только б/у-стоп")
    ap.add_argument("--trigger", default="35",
                    help="точки срабатывания (%% пути до TP)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit-days", type=float, default=None)
    ap.add_argument("--has-oi", type=int, default=1)
    ap.add_argument("--no-cb", action="store_true",
                    help="выключить Circuit Breaker — изолирует эффект правила выхода "
                         "от обратной связи через серию убытков (при qty=0 безубыток "
                         "становится микро-убытком и кормит счётчик)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    qtys = [float(x) for x in args.qty.split(",")]
    triggers = [float(x) for x in args.trigger.split(",")]

    base = Settings.from_yaml(args.config)
    risk = VIRTUAL_BALANCE * (base.trading.risk_per_trade_pct / 100)

    t0 = time.time()
    log(f"Loading {args.db} ...")
    data = load_data(args.db, limit_days=args.limit_days)
    log(f"Data loaded in {time.time() - t0:.1f}s; "
        f"{len(qtys) * len(triggers)} configs to run")

    rows = []
    for trig in triggers:
        for qty in qtys:
            t1 = time.time()
            s = Settings.from_yaml(args.config)
            s.trading.partial_close_pct = trig
            s.trading.partial_close_qty_pct = qty
            if args.no_cb:
                s.trading.circuit_breaker_enabled = False
            r = simulate(s, data, has_oi=bool(args.has_oi), collect_retracement=False)
            outcomes = summarize(r["trades_list"], risk)
            total_r = r["total_pnl"] / risk
            row = {
                "trigger_pct": trig,
                "qty_pct": qty,
                "trades": r["trades"],
                "win_rate": r["win_rate"],
                "total_pnl": r["total_pnl"],
                "total_R": round(total_r, 2),
                "R_per_trade": round(total_r / r["trades"], 4) if r["trades"] else 0.0,
                "total_fees": r["total_fees"],
                "outcomes": outcomes,
            }
            rows.append(row)
            log(f"trigger={trig}% qty={qty}%: trades={r['trades']} "
                f"WR={r['win_rate']}% R={total_r:+.2f} "
                f"R/сделку={row['R_per_trade']:+.4f} ({time.time() - t1:.1f}s)")
            for cat, o in sorted(outcomes.items()):
                log(f"    {cat}: n={o['n']} R={o['R']:+.2f}")
            with open(out_dir / f"be_t{trig:g}_q{qty:g}.json", "w") as f:
                json.dump(r, f, indent=2, default=str)

    with open(out_dir / "summary.json", "w") as f:
        json.dump(rows, f, indent=2)

    log("")
    log(f"{'триггер':>8} {'доля':>6} {'сделок':>7} {'WR':>6} {'сумма R':>9} {'R/сделку':>10}")
    for r in rows:
        log(f"{r['trigger_pct']:>7g}% {r['qty_pct']:>5g}% {r['trades']:>7} "
            f"{r['win_rate']:>5}% {r['total_R']:>+9.2f} {r['R_per_trade']:>+10.4f}")
    log(f"TOTAL elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
