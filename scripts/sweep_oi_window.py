"""Окно OI-гейта: 3 точки против временного окна. Baseline и вариант в одной сетке.

    .venv/bin/python scripts/sweep_oi_window.py data/trading_bot_27.08-22.09.db OUT main
    .venv/bin/python scripts/sweep_oi_window.py data/trading_bot_10.08-25.08.db OUT arch
    .venv/bin/python scripts/sweep_oi_window.py data/trading_bot.db OUT fresh
    .venv/bin/python scripts/sweep_pool.py OUT/oiw_arch.json OUT/oiw_main.json

Зачем. `OI_TREND_BARS = 3` — это число СТРОК, а строка пишется раз в цикл сбора,
поэтому окно гейта равно двум кадансам и задаётся скоростью бота, а не
стратегией. Замер 23.09.2026 (медиана размаха 3 точек на момент сигнала):
10-25.08 — 557 с, 27.08-22.09 — 187 с, свежая БД — 139 с. Замысел был
`sustain_bars * timeframe` = 12 мин; фактически осталось 19% от него, и величина
уехала вчетверо от одних перф-фиксов цикла.

Отсюда две вещи, которые скрипт обязан разделить. Первая — определение окна
(число точек против времени). Вторая — строгость порога: `oi_slope_min_pct = 2`
на окне 2.3 мин это 0.87 %/мин, то есть на 12-минутном окне эквивалент ~10%, а
не 2%. Поэтому временное окно гоняется на сетке порогов, иначе разница окажется
разницей строгости.

ВАЖНО: в боевом конфиге гейт выключен (`oi_filter_enabled: false`), поэтому
клетка `prod` — это прогон БЕЗ гейта, и она обязана совпасть посделочно до и
после правки. Это и есть проверка по правилу 3 AGENTS.md: сначала прогон на
ветке ДО изменений, потом сравнение.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, log, simulate
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
CONFIG = sys.argv[4] if len(sys.argv) > 4 else "config/config.yaml"

# (метка, оверрайды strategy). Клетки с `oi_trend_window_bars` пропускаются на
# ветке до правки — там этого параметра ещё нет.
CELLS = (
    ("prod", {}),
    # Прежний режим и временное окно — каждый по сетке порогов. Сравнивать их
    # при ОДНОМ пороге нельзя: величина разная. Порог, дающий одинаковую долю
    # прохождения (замер по живым сигналам 23.09.2026): прежний 2% на БД
    # 27.08-22.09 = окно 3.2%, прежний 3% на 10-25.08 = окно 3.25%. Сам этот
    # факт — довод за окно: у него один порог на оба периода, у прежнего режима
    # два разных.
    ("3 точки, порог 1%", {"oi_filter_enabled": True, "oi_slope_min_pct": 1.0}),
    ("3 точки, порог 2%", {"oi_filter_enabled": True, "oi_slope_min_pct": 2.0}),
    ("3 точки, порог 3%", {"oi_filter_enabled": True, "oi_slope_min_pct": 3.0}),
    ("3 точки, порог 5%", {"oi_filter_enabled": True, "oi_slope_min_pct": 5.0}),
    ("окно 12 мин, порог 1%", {"oi_filter_enabled": True, "oi_trend_window_bars": 4,
                               "oi_slope_min_pct": 1.0}),
    ("окно 12 мин, порог 2%", {"oi_filter_enabled": True, "oi_trend_window_bars": 4,
                               "oi_slope_min_pct": 2.0}),
    ("окно 12 мин, порог 3%", {"oi_filter_enabled": True, "oi_trend_window_bars": 4,
                               "oi_slope_min_pct": 3.0}),
    ("окно 12 мин, порог 4%", {"oi_filter_enabled": True, "oi_trend_window_bars": 4,
                               "oi_slope_min_pct": 4.0}),
    ("окно 12 мин, порог 5%", {"oi_filter_enabled": True, "oi_trend_window_bars": 4,
                               "oi_slope_min_pct": 5.0}),
)

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток")

out = {"db": DB, "days": days, "configs": {}}
for label, over in CELLS:
    s = Settings.from_yaml(CONFIG)
    missing = [k for k in over if not hasattr(s.strategy, k)]
    if missing:
        log(f"  {label:<34} пропущено: нет параметра {', '.join(missing)}")
        continue
    for k, v in over.items():
        setattr(s.strategy, k, v)
    r = simulate(s, data, has_oi=True, collect_retracement=False)
    out["configs"][label] = {
        "overrides": over,
        "trades": [{"symbol": t["symbol"], "entry_time": str(t["entry_time"]),
                    "R": t["pnl"] / t["risk"]} for t in r["trades_list"] if t.get("risk")],
        "signals": r["signals"], "win_rate": r["win_rate"], "total_R": r["total_R"],
        "max_drawdown_R": r["max_drawdown_R"], "full_take_rate": r.get("full_take_rate"),
    }
    log(f"  {label:<34} сигналов={r['signals']:4d}  сделок={r['trades']:3d}  "
        f"тейков={r.get('full_take_rate') or 0:>5.1f}%  ΣR={r['total_R']:+7.2f}  "
        f"R/сут={r['total_R']/days:+.3f}  просад={r['max_drawdown_R']:5.2f}R")

json.dump(out, open(f"{OUT}/oiw_{TAG}.json", "w"), indent=1, ensure_ascii=False)
log(f"готово за {time.time()-t0:.0f}s -> {OUT}/oiw_{TAG}.json")
