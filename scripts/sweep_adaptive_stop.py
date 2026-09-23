"""Концепт (б): стоп как ЧИСЛО РАЗМАХОВ sustain-окна вместо фиксированного %.

    .venv/bin/python scripts/sweep_adaptive_stop.py data/trading_bot_27.08-22.09.db OUT main
    .venv/bin/python scripts/sweep_adaptive_stop.py data/trading_bot_10.08-25.08.db OUT arch
    .venv/bin/python scripts/sweep_pool.py OUT/fin_arch.json OUT/fin_main.json

Зачем. Фиксированные 5% — это от 1.4 до 3.8 размаха sustain-окна в зависимости
от монеты (замер 23.09.2026: размах гуляет 0.95-5.0%). Здесь стоп привязан к
шуму самой монеты: `stop = mult × размах`, с границами 2-10%
(`analytics.utils.adaptive_stop_pct`).

Это НЕ ATR-адаптивный стоп из `docs/decisions.md`: там мерой был ИСТОРИЧЕСКИЙ
ATR, непоказательный в момент пампа. Здесь мера — размах того же sustain-окна,
который детектор уже считает для `max_window_range_pct`.

mult = 0 обязан воспроизвести боевой конфиг посделочно: это встроенная проверка
parity по правилу 3 AGENTS.md.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, log, simulate
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
CONFIG = sys.argv[4] if len(sys.argv) > 4 else "config/config.yaml"
VALUES = (0.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5)

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток")

out = {"db": DB, "days": days, "configs": {}}
for v in VALUES:
    s = Settings.from_yaml(CONFIG)
    s.trading.backtest_deposit_usdt = 1000.0
    s.trading.stop_loss_window_range_mult = v
    r = simulate(s, data, has_oi=True, collect_retracement=False, markets=None)
    label = "prod" if v == 0.0 else f"стоп = {v:g} размаха"
    out["configs"][label] = {
        "overrides": {"trading.stop_loss_window_range_mult": v},
        "trades": [{"symbol": t["symbol"], "entry_time": str(t["entry_time"]),
                    "R": t["pnl"] / t["risk"]} for t in r["trades_list"] if t.get("risk")],
        "win_rate": r["win_rate"], "total_R": r["total_R"],
        "max_drawdown_R": r["max_drawdown_R"],
        "full_take_rate": r.get("full_take_rate"),
    }
    log(f"{label:<22} сделок={r['trades']:3d}  полных тейков="
        f"{r.get('full_take_rate') or 0:>5.1f}%  ΣR={r['total_R']:+7.2f}  "
        f"R/сут={r['total_R']/days:+.3f}  просад={r['max_drawdown_R']:.2f}R")

json.dump(out, open(f"{OUT}/fin_{TAG}.json", "w"), indent=1, ensure_ascii=False)
log(f"готово за {time.time()-t0:.0f}s -> {OUT}/fin_{TAG}.json")
