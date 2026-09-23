"""Посделочное сравнение двух прогонов одной сетки: до правки и после.

    .venv/bin/python scripts/sweep_oi_parity.py BASE_DIR NEW_DIR

Правило 3 AGENTS.md: parity проверяется прогоном на ветке ДО изменений, а не
двумя прогонами после. Здесь сверяются клетки, которые есть в обоих прогонах
(`prod` и прежний режим гейта) — они обязаны совпасть до последней сделки,
иначе правка задела не только окно OI. Клетки, которых на старой ветке не было,
печатаются отдельно как новые.
"""
import json, sys
from pathlib import Path

base_dir, new_dir = Path(sys.argv[1]), Path(sys.argv[2])
bad = 0

for new_path in sorted(new_dir.glob("oiw_*.json")):
    new = json.load(open(new_path))
    base_path = base_dir / new_path.name
    if not base_path.exists():
        print(f"{new_path.name}: baseline отсутствует — сравнивать не с чем")
        continue
    base = json.load(open(base_path))
    print(f"\n{new_path.name}  ({base['days']:.1f} сут)")
    for label, cfg in new["configs"].items():
        if label not in base["configs"]:
            print(f"  {label:<34} НОВАЯ клетка: сделок {len(cfg['trades'])}, "
                  f"ΣR {cfg['total_R']:+.2f}")
            continue
        a = [(t["symbol"], t["entry_time"], round(t["R"], 9)) for t in base["configs"][label]["trades"]]
        b = [(t["symbol"], t["entry_time"], round(t["R"], 9)) for t in cfg["trades"]]
        if a == b:
            print(f"  {label:<34} совпадает посделочно ({len(a)} сделок)")
        else:
            bad += 1
            only_a, only_b = set(a) - set(b), set(b) - set(a)
            print(f"  {label:<34} РАСХОЖДЕНИЕ: было {len(a)}, стало {len(b)}, "
                  f"только до {len(only_a)}, только после {len(only_b)}")
            for x in list(only_a)[:5]:
                print(f"      только до:    {x}")
            for x in list(only_b)[:5]:
                print(f"      только после: {x}")

print("\nparity нарушен" if bad else "\nparity подтверждён: общие клетки совпали посделочно")
sys.exit(1 if bad else 0)
