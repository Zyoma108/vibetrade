"""Парное сравнение режимов окна OI при сопоставимой строгости, по двум базам.

    .venv/bin/python scripts/sweep_oi_pairs.py OUT/oiw_main.json OUT/oiw_arch.json

Сравнивать «3 точки, порог 2%» с «окно 12 мин, порог 2%» напрямую нельзя: это
разные величины, и разница окажется разницей строгости. Поэтому каждая пара
подбирается по ЧИСЛУ СДЕЛОК: из клеток окна берётся та, что ближе всего по
объёму выборки к клетке прежнего режима. Сравнение парное (`metrics.compare`),
общие сделки дают ровно ноль — правило 8 AGENTS.md.
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.metrics import compare

for path in sys.argv[1:]:
    src = json.load(open(path))
    cells = src["configs"]
    legacy = {k: v for k, v in cells.items() if "точ" in k}
    window = {k: v for k, v in cells.items() if "окно" in k}
    print(f"\n{src['db']}  ({src['days']:.1f} сут, боевой без гейта: "
          f"{len(cells['prod']['trades'])} сделок, ΣR {cells['prod']['total_R']:+.2f})")
    print(f"  {'прежний режим':<22} {'ближайшее окно':<24} {'сделок':>13} "
          f"{'ΣR прежн':>9} {'ΣR окно':>9} {'разница ΣR (95% ДИ)':>28}")
    for lk, lv in legacy.items():
        if not window:
            continue
        n = len(lv["trades"])
        wk = min(window, key=lambda k: abs(len(window[k]["trades"]) - n))
        wv = window[wk]
        # `compare` ждёт сделки движка; здесь есть только R — подставляем его
        # как pnl при risk = 1, что даёт ровно тот же R.
        mk = lambda tr: [{"pnl": t["R"], "risk": 1.0, "symbol": t["symbol"],
                          "entry_time": t["entry_time"]} for t in tr]
        r = compare(mk(wv["trades"]), mk(lv["trades"]), days=src["days"])
        ci = r["delta_R_ci"]
        ci_s = f"[{ci[0]:+.2f}; {ci[1]:+.2f}]" if ci else "—"
        star = "*" if r["significant"] else " "
        print(f"  {lk:<22} {wk:<24} {n:>5} / {len(wv['trades']):<5} "
              f"{lv['total_R']:>+9.2f} {wv['total_R']:>+9.2f} "
              f"  {r['delta_R']:>+7.2f} {ci_s:>18}{star}")

print("\n* — интервал разницы не накрывает ноль. Разница считается ОТ прежнего режима "
      "К окну: плюс = окно лучше.")
