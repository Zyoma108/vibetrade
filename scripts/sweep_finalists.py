"""Финалисты этапа 3: выгрузка посделочных результатов для объединённой оценки.

    .venv/bin/python scripts/sweep_finalists.py data/trading_bot.db OUT main
    .venv/bin/python scripts/sweep_finalists.py data/trading_bot_10.08-25.08.db OUT arch
    .venv/bin/python scripts/sweep_pool.py OUT/fin_main.json OUT/fin_arch.json

Зачем отдельный прогон. `sweep_stage3.py` сохраняет только агрегаты, а
объединить две базы по агрегатам нельзя: нужна посделочная разница с боевым
конфигом и дата входа каждой сделки, чтобы блочный bootstrap ресэмплил сутки
по обеим базам разом.

Зачем объединять. 26 суточных блоков дают полуширину интервала ~13R при
эффекте ~11R — выборка физически не различает кандидатов. Объединение двух
периодов (~41 сутки) не заменяет out-of-sample проверку (её делает
`sweep_crosscheck.py`), а отвечает на другой вопрос: существует ли эффект
вообще. Периоды разные по режиму рынка — блочный ресэмпл трактует это как
часть разброса, а не прячет.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, load_markets, log, simulate
from src.config import Settings

DB, OUT, TAG = sys.argv[1], sys.argv[2], sys.argv[3]
CONFIG = sys.argv[4] if len(sys.argv) > 4 else "config/config.yaml"
# Этап 2: реальный депозит и шаг лота биржи. Отдельно от этапа 1 потому, что
# оптимум стратегии не должен подгоняться под квантование лота на конкретных
# монетах — но применять конфиг можно только тот, который на реальном
# депозите исполним.
DEPOSIT = float(sys.argv[5]) if len(sys.argv) > 5 else 1000.0
MARKETS = load_markets(sys.argv[6]) if len(sys.argv) > 6 else None

# Финалисты отобраны по СОВПАДЕНИЮ ЗНАКА на обеих базах, а не по лучшей цифре
# на главной: ячейки RR3 x партиал 20-25%, лучшие на 27.08-22.09, на архивной
# базе уходят в минус. Воспроизводятся две вещи — вся строка RR 4 (7 плюсов из
# 7 на обеих) и весь столбец партиала 70% (5 из 5), но второе роняет winrate до
# 45-55%. Единственная ячейка, положительная на обеих И удерживающая WR>=65%
# на обеих, — RR 4 x партиал 20%.
FINALISTS = {
    "prod": {},
    "RR4 x партиал 20%": {"trading.risk_reward_ratio": 4.0, "trading.partial_close_pct": 20.0},
    "RR4 x партиал 25%": {"trading.risk_reward_ratio": 4.0, "trading.partial_close_pct": 25.0},
    "доля партиала 10%": {"trading.partial_close_qty_pct": 10.0},
    "доля партиала 20%": {"trading.partial_close_qty_pct": 20.0},
    "стоп 4%": {"trading.stop_loss_pct": 4.0},
    "RR4 x п20% + доля 10%": {"trading.risk_reward_ratio": 4.0,
                              "trading.partial_close_pct": 20.0,
                              "trading.partial_close_qty_pct": 10.0},
    "RR4 x п20% + доля 10% + стоп 4%": {"trading.risk_reward_ratio": 4.0,
                                        "trading.partial_close_pct": 20.0,
                                        "trading.partial_close_qty_pct": 10.0,
                                        "trading.stop_loss_pct": 4.0},
}

t0 = time.time()
data = load_data(DB)
days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
log(f"загрузка {time.time()-t0:.0f}s; период {days:.1f} суток; депозит ${DEPOSIT:.0f}; шаг лота {'вкл' if MARKETS else 'выкл'}")

out = {"db": DB, "days": days, "configs": {}}
for label, over in FINALISTS.items():
    s = Settings.from_yaml(CONFIG)
    s.trading.backtest_deposit_usdt = DEPOSIT
    for path, val in over.items():
        section, field = path.split(".")
        setattr(getattr(s, section), field, val)
    r = simulate(s, data, has_oi=True, collect_retracement=False, markets=MARKETS)
    out["configs"][label] = {
        "overrides": over,
        "trades": [{"symbol": t["symbol"], "entry_time": str(t["entry_time"]),
                    "R": t["pnl"] / t["risk"]} for t in r["trades_list"] if t.get("risk")],
        "win_rate": r["win_rate"], "total_R": r["total_R"],
        "max_drawdown_R": r["max_drawdown_R"],
        "amount_too_small": r["amount_too_small"],
        "partial_unavailable": r["partial_unavailable"],
    }
    log(f"{label:<32} сделок={r['trades']:3d} WR={r['win_rate']:5.1f}% ΣR={r['total_R']:+7.2f}"
        f"  отказов по лоту={r['amount_too_small']:3d} партиал недоступен={r['partial_unavailable']:3d}")

json.dump(out, open(f"{OUT}/fin_{TAG}.json", "w"), indent=1, ensure_ascii=False)
log(f"готово за {time.time()-t0:.0f}s -> {OUT}/fin_{TAG}.json")
