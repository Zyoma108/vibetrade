"""Объединённая оценка финалистов по двум базам: один блочный bootstrap по суткам.

    .venv/bin/python scripts/sweep_pool.py OUT/fin_arch.json OUT/fin_main.json

Считается разница с боевым конфигом В ПЕРЕСЧЁТЕ НА СУТКИ: периоды разной
длины, и суммарный R несопоставим между базами напрямую. Блок ресэмпла —
пара (база, дата входа), так что сутки одной базы не подменяют сутки другой.

Это НЕ замена out-of-sample проверке. Она отвечает «переносится ли эффект»,
объединение — «существует ли он вообще при том объёме данных, что есть».
Применять стоит только то, что прошло обе.
"""
import json, random, statistics as st, sys

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.metrics import BOOTSTRAP_RESAMPLES, BOOTSTRAP_SEED

sources = [json.load(open(p)) for p in sys.argv[1:]]
labels = [lab for lab in sources[0]["configs"] if lab != "prod"]
total_days = sum(s["days"] for s in sources)

print(f"объединено баз: {len(sources)}, суток суммарно {total_days:.1f}")
for s in sources:
    print(f"  {s['db']}: {s['days']:.1f} сут, боевой ΣR {s['configs']['prod']['total_R']:+.2f}, "
          f"WR {s['configs']['prod']['win_rate']:.1f}%")
print()
print(f"{'конфигурация':<32} {'ΣR по базам':>22} {'WR':>14} {'Δ R/сут (95% ДИ)':>28}")

for label in labels:
    # (блок, разница в R) по каждой сделке, которую взяла хоть одна конфигурация
    pairs, per_db = [], []
    for src in sources:
        a = {(t["symbol"], t["entry_time"]): t["R"] for t in src["configs"][label]["trades"]}
        b = {(t["symbol"], t["entry_time"]): t["R"] for t in src["configs"]["prod"]["trades"]}
        tag = src["db"]
        for k in set(a) | set(b):
            pairs.append((f"{tag}|{k[1][:10]}", a.get(k, 0.0) - b.get(k, 0.0)))
        cfg = src["configs"][label]
        per_db.append((cfg["total_R"], cfg["win_rate"], cfg["max_drawdown_R"]))

    grouped: dict = {}
    for block, diff in pairs:
        grouped.setdefault(block, []).append(diff)
    keys = sorted(grouped)
    # Сумма разницы по блоку, делённая на число блоков = Δ R в сутки.
    block_sums = [sum(grouped[k]) for k in keys]
    point = sum(block_sums) / len(keys)
    rnd = random.Random(BOOTSTRAP_SEED)
    means = sorted(st.fmean(rnd.choices(block_sums, k=len(block_sums)))
                   for _ in range(BOOTSTRAP_RESAMPLES))
    lo, hi = means[int(0.025 * BOOTSTRAP_RESAMPLES)], means[int(0.975 * BOOTSTRAP_RESAMPLES)]
    star = "*" if lo > 0 or hi < 0 else " "
    print(f"{label:<32} {'/'.join(f'{r:+.1f}' for r, _, _ in per_db):>22} "
          f"{'/'.join(f'{w:.0f}%' for _, w, _ in per_db):>14} "
          f"{point:+.3f} [{lo:+.3f}; {hi:+.3f}]{star:>2}")

print()
print(f"блоков в ресэмпле: {len(keys)} (сутки, в которые хоть одна конфигурация торговала)")
print("* — интервал разницы не накрывает ноль.")
