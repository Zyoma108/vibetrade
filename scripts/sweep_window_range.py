"""Концепт (а): жёсткость `max_window_range_pct` — срезать самые шумные сетапы.

    .venv/bin/python scripts/sweep_window_range.py data/trading_bot_27.08-22.09.db OUT main
    .venv/bin/python scripts/sweep_window_range.py data/trading_bot_10.08-25.08.db OUT arch
    .venv/bin/python scripts/sweep_pool.py OUT/fin_arch.json OUT/fin_main.json

Зачем. Стоп зафиксирован на 5% от входа, а размах sustain-окна у монет гуляет
от 0.95% до 5.0% — то есть один и тот же стоп стоит от 1.4 до 3.8 размаха.
Замер 23.09.2026 на двух БД: самый шумный квартиль (стоп ~1.4 размаха, то есть
стоп внутри шума) — худший на ОБЕИХ базах (E[R] -0.001 против +0.253 на
27.08-22.09 и +0.434 против +0.973 на 10-25.08). Где оптимум — не
воспроизвелось, поэтому проверяется срезание худшего хвоста, а не подгонка
коэффициента. Правильная версия той же идеи — стоп пропорционально размаху
(концепт «б»), она требует правки движка и идёт отдельно.

Формат вывода совместим с `sweep_pool.py`: ключ конфигурации, посделочные R и
дата входа, чтобы объединить обе базы одним блочным bootstrap по суткам.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, log, simulate
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
CONFIG = sys.argv[4] if len(sys.argv) > 4 else "config/config.yaml"

# 0 = фильтр выключен. Боевое значение 4.0. Верхняя граница шумного квартиля по
# замеру — 2.84% (27.08-22.09) и 2.98% (10-25.08), поэтому плотная сетка там.
VALUES = (0.0, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0, 5.0)

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток")

out = {"db": DB, "days": days, "configs": {}}
for v in VALUES:
    s = Settings.from_yaml(CONFIG)
    s.trading.backtest_deposit_usdt = 1000.0
    s.strategy.max_window_range_pct = v
    r = simulate(s, data, has_oi=True, collect_retracement=False, markets=None)
    label = "prod" if v == s.strategy.max_window_range_pct and v == 4.0 else f"размах <= {v:g}%"
    if v == 4.0:
        label = "prod"
    out["configs"][label] = {
        "overrides": {"strategy.max_window_range_pct": v},
        "trades": [{"symbol": t["symbol"], "entry_time": str(t["entry_time"]),
                    "R": t["pnl"] / t["risk"]} for t in r["trades_list"] if t.get("risk")],
        "win_rate": r["win_rate"], "total_R": r["total_R"],
        "max_drawdown_R": r["max_drawdown_R"],
        "full_take_rate": r.get("full_take_rate"),
    }
    log(f"размах <= {v:>4g}%  сделок={r['trades']:3d}  полных тейков="
        f"{r.get('full_take_rate') or 0:>5.1f}%  ΣR={r['total_R']:+7.2f}  "
        f"E[R]={r['expectancy_R'] if r['expectancy_R'] is not None else 0:+.4f}  "
        f"просад={r['max_drawdown_R']:.2f}R")

json.dump(out, open(f"{OUT}/fin_{TAG}.json", "w"), indent=1, ensure_ascii=False)
log(f"готово за {time.time()-t0:.0f}s -> {OUT}/fin_{TAG}.json")
