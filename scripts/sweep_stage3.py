"""Программа свипов этапа 3: одна загрузка БД, все семейства, парные сравнения.

Запуск (прогон одной БД ~30 мин: загрузка ~200 с + 62 конфигурации по ~25 с):

    .venv/bin/python scripts/sweep_stage3.py data/trading_bot.db /tmp/vt_sweep main
    .venv/bin/python scripts/sweep_stage3.py data/trading_bot_10.08-25.08.db /tmp/vt_sweep arch
    .venv/bin/python scripts/sweep_crosscheck.py /tmp/vt_sweep/arch.json \
        /tmp/vt_sweep/main.json "10-25.08" "27.08-22.09"

Базы гонять ПОСЛЕДОВАТЕЛЬНО: загрузка держит ~5 ГБ.

Стадия 1 — чистая модель: депозит $1000, шаг лота выключен. Оптимум не должен
подгоняться под квантование лота на конкретных монетах: это артефакт
исполнения, а не свойство стратегии. Проверка победителя на реальном депозите
делается отдельно.

Решения принимаются по правилу 8 AGENTS.md: парное сравнение с боевым конфигом,
блочный bootstrap по суткам, звёздочка = интервал разницы не накрывает ноль.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, log, simulate
from src.backtest.metrics import SIGNIFICANCE_LEGEND, fmt, format_delta
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
CONFIG = sys.argv[4] if len(sys.argv) > 4 else "config/config.yaml"

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток")

BASE = Settings.from_yaml(CONFIG)
BASE.trading.backtest_deposit_usdt = 1000.0

runs, rows = {}, []


def run(key, label, **over):
    s = Settings.from_yaml(CONFIG)
    s.trading.backtest_deposit_usdt = 1000.0
    for path, val in over.items():
        section, field = path.split(".")
        setattr(getattr(s, section), field, val)
    t = time.time()
    r = simulate(s, data, has_oi=True, collect_retracement=False, markets=None)
    runs[key] = r["trades_list"]
    row = {
        "key": key, "label": label, "family": key[0], **over,
        "signals": r["signals"], "trades": r["trades"], "win_rate": r["win_rate"],
        "total_R": r["total_R"], "R_per_trade": r["expectancy_R"],
        "R_ci": r["expectancy_R_ci"], "R_per_day": r.get("R_per_day"),
        "max_drawdown_R": r["max_drawdown_R"], "worst_loss_streak": r["worst_loss_streak"],
        "tp": r["tp_wins"], "sl": r["sl_losses"], "time": r["time_exits"],
        "partials": r["partials"], "elapsed": round(time.time() - t, 1),
    }
    rows.append(row)
    log(f"{label:<34} сделок={r['trades']:3d} WR={r['win_rate']:5.1f}% "
        f"ΣR={fmt(r['total_R']):>7} E[R]={fmt(r['expectancy_R'], '+.4f')} ({row['elapsed']:.0f}s)")
    return row


PROD = ("prod",)
run(PROD, "БОЕВОЙ КОНФИГ")

# 1. RR x порог партиала. Порог задан долей пути до TP, поэтому свип RR молча
#    двигает и защиту — двумерная сетка разделяет эти два эффекта.
for rr in (2.0, 2.5, 3.0, 3.5, 4.0):
    for pc in (20.0, 25.0, 30.0, 35.0, 45.0, 55.0, 70.0):
        abs_trigger = rr * BASE.trading.stop_loss_pct * pc / 100
        run(("grid", rr, pc), f"RR {rr:g} x партиал {pc:g}% (+{abs_trigger:.1f}%)",
            **{"trading.risk_reward_ratio": rr, "trading.partial_close_pct": pc})

# 2. Доля позиции, закрываемая по триггеру
for q in (0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 70.0):
    run(("qty", q), f"доля партиала {q:g}%", **{"trading.partial_close_qty_pct": q})

# 3. Стоп-лосс
for sl in (3.0, 4.0, 5.0, 6.0, 7.0, 8.0):
    run(("sl", sl), f"стоп {sl:g}%", **{"trading.stop_loss_pct": sl})

# 4. Circuit Breaker
NEVER = 999
for label, en, red, stop, mult in (
    ("CB выкл", False, 2, 3, 50.0),
    ("CB только reduce", True, 2, NEVER, 50.0),
    ("CB только stop", True, NEVER, 3, 50.0),
    ("CB мягкий 3/4/70%", True, 3, 4, 70.0),
):
    run(("cb", label), label, **{
        "trading.circuit_breaker_enabled": en,
        "trading.circuit_breaker_loss_streak_reduce": red,
        "trading.circuit_breaker_loss_streak_stop": stop,
        "trading.circuit_breaker_reduce_mult_pct": mult,
    })

# 5. Порог ликвидности и число слотов
for th in (0.0, 10000.0, 15000.0, 20000.0, 25000.0, 35000.0, 50000.0):
    run(("minvol", th), f"ликвидность {th:g}", **{"strategy.min_baseline_volume_usdt": th})
for mp in (5, 8, 10, 15, 20):
    run(("slots", mp), f"слотов {mp}", **{"trading.max_positions": mp})

# --- парные сравнения ---
for row in rows:
    if row["key"] == PROD:
        continue
    cell, cmp = format_delta(runs[row["key"]], runs[PROD], days=days)
    row["delta_cell"], row["vs_prod"] = cell, cmp

json.dump({"db": DB, "days": days, "rows": rows}, open(f"{OUT}/{TAG}.json", "w"),
          indent=1, ensure_ascii=False, default=str)

log("")
log(f"{'конфигурация':<34} {'сдел':>5} {'WR':>6} {'ΣR':>8} {'R/сут':>7} "
    f"{'просад':>7} {'Δ R к боевому (95% ДИ)':>30}")
prod = next(r for r in rows if r["key"] == PROD)
log(f"{prod['label']:<34} {prod['trades']:>5} {prod['win_rate']:>5.1f}% "
    f"{fmt(prod['total_R']):>8} {fmt(prod['R_per_day'], '+.3f'):>7} "
    f"{prod['max_drawdown_R']:>6.2f}R {'— (база)':>30}")
for fam in ("grid", "qty", "sl", "cb", "minvol", "slots"):
    log("")
    for row in [r for r in rows if r["key"][0] == fam]:
        star = "*" if row["vs_prod"]["significant"] else " "
        log(f"{row['label']:<34} {row['trades']:>5} {row['win_rate']:>5.1f}% "
            f"{fmt(row['total_R']):>8} {fmt(row['R_per_day'], '+.3f'):>7} "
            f"{row['max_drawdown_R']:>6.2f}R {row['delta_cell']:>30}")
log("")
log(SIGNIFICANCE_LEGEND)
log(f"ВСЕГО: {len(rows)} конфигураций за {time.time()-t0:.0f}s")
