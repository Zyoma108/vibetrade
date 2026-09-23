"""Стоп 3.5-5% в двух стадиях: чистая модель и реальный депозит с шагом лота.

    .venv/bin/python scripts/sweep_stop_deposit.py data/trading_bot_27.08-22.09.db OUT main 60
    .venv/bin/python scripts/sweep_stop_deposit.py data/trading_bot_10.08-25.08.db OUT arch 60
    .venv/bin/python scripts/sweep_pool.py OUT/fin_arch_dep.json OUT/fin_main_dep.json

Зачем две стадии в одном прогоне. Оптимум стопа зависит от депозита, и зависит
в противоположные стороны: более тесный стоп даёт больший размер позиции на тот
же бюджет риска, а значит чаще упирается в минимальный лот биржи. Замер
22.09.2026: стоп 4% давал +0.169 R/сут на чистой модели и +0.041 на депозите
$55, потому что отказов `amount_too_small` стало 4 вместо 1. Сравнивать стадии
надо на ОДНОЙ сетке, иначе разницу не отделить от разницы сеток.

Мотивация самой сетки: две независимые параметризации сошлись на том, что
боевой стоп 5% широк. Свип этапа 3 по фиксированному стопу дал 4% лучше 5% на
обеих БД (знак 6/6, чистая одномодальная форма); свип адаптивного стопа
(`sweep_adaptive_stop.py`) дал оптимум на 1.5-1.75 размаха окна, что при
медианном размахе 2.3% означает 3.5-4.0%. Здесь сетка сгущена там, где оба
указывают.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import DEFAULT_MARKETS_PATH, load_data, load_markets, log, simulate
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
DEPOSIT = float(sys.argv[4]) if len(sys.argv) > 4 else 60.0
CONFIG = sys.argv[5] if len(sys.argv) > 5 else "config/config.yaml"
VALUES = (3.5, 3.75, 4.0, 4.25, 4.5, 4.75, 5.0)   # 5.0 = боевой

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
markets = load_markets(DEFAULT_MARKETS_PATH)
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток; инструментов {len(markets or {})}")

for stage, deposit, mk in (("clean", 1000.0, None), ("dep", DEPOSIT, markets)):
    out = {"db": DB, "days": days, "configs": {}}
    log(f"--- стадия {stage}: депозит ${deposit:.0f}, шаг лота {'вкл' if mk else 'выкл'} ---")
    for v in VALUES:
        s = Settings.from_yaml(CONFIG)
        s.trading.backtest_deposit_usdt = deposit
        s.trading.stop_loss_pct = v
        r = simulate(s, data, has_oi=True, collect_retracement=False, markets=mk)
        label = "prod" if v == 5.0 else f"стоп {v:g}%"
        out["configs"][label] = {
            "overrides": {"trading.stop_loss_pct": v},
            "trades": [{"symbol": t["symbol"], "entry_time": str(t["entry_time"]),
                        "R": t["pnl"] / t["risk"]} for t in r["trades_list"] if t.get("risk")],
            "win_rate": r["win_rate"], "total_R": r["total_R"],
            "max_drawdown_R": r["max_drawdown_R"],
            "full_take_rate": r.get("full_take_rate"),
            "amount_too_small": r["amount_too_small"],
            "partial_unavailable": r["partial_unavailable"],
        }
        log(f"  стоп {v:>4g}%  сделок={r['trades']:3d}  тейков={r.get('full_take_rate') or 0:>5.1f}%  "
            f"ΣR={r['total_R']:+7.2f}  R/сут={r['total_R']/days:+.3f}  просад={r['max_drawdown_R']:5.2f}R  "
            f"отказ по лоту={r['amount_too_small']:3d}  партиал недоступен={r['partial_unavailable']:3d}")
    json.dump(out, open(f"{OUT}/fin_{TAG}_{stage}.json", "w"), indent=1, ensure_ascii=False)

log(f"готово за {time.time()-t0:.0f}s -> {OUT}/fin_{TAG}_*.json")
