"""Сверка двух свипов: что подтвердилось на ОБЕИХ БД.

Правило отбора (согласовано 22.09.2026): в прод едет только то, что улучшает
на обеих базах И чей интервал разницы не накрывает ноль хотя бы на одной, не
ухудшая значимо другую. Периоды не пересекаются: 10-25.08 и 27.08-22.09.
"""
import json, sys

a = json.load(open(sys.argv[1]))   # первая БД
b = json.load(open(sys.argv[2]))   # вторая БД
LA, LB = sys.argv[3], sys.argv[4]

def index(d):
    return {str(r["key"]): r for r in d["rows"]}

A, B = index(a), index(b)
prod_key = str(("prod",))

print(f"{'':<34} {LA:>26}   {LB:>26}")
print(f"{'конфигурация':<34} {'Δ R':>10} {'95% ДИ':>15}   {'Δ R':>10} {'95% ДИ':>15}  вердикт")

def cell(row):
    if row is None or "vs_prod" not in row:
        return "—".rjust(10), "".rjust(15), None, False
    c = row["vs_prod"]
    ci = c["delta_R_ci"]
    return (f"{c['delta_R']:+.2f}".rjust(10),
            (f"[{ci[0]:+.1f};{ci[1]:+.1f}]" if ci else "—").rjust(15),
            c["delta_R"], c["significant"])

winners = []
for fam in ("grid", "qty", "sl", "cb", "minvol", "slots"):
    keys = [k for k in A if k.startswith(f"('{fam}'")]
    if not keys:
        continue
    print()
    for k in sorted(keys, key=lambda k: -(A[k]["vs_prod"]["delta_R"] if "vs_prod" in A[k] else -1e9)):
        ra, rb = A.get(k), B.get(k)
        ca, cia, da, sa = cell(ra)
        cb_, cib, db_, sb = cell(rb)
        if da is None or db_ is None:
            verdict = "нет пары"
        elif da > 0 and db_ > 0:
            verdict = "ЛУЧШЕ НА ОБЕИХ" + ("*" if (sa or sb) else " (не значимо)")
            if sa or sb:
                winners.append((k, ra["label"], da, db_, sa, sb))
        elif da < 0 and db_ < 0:
            verdict = "хуже на обеих"
        else:
            verdict = "противоречие"
        print(f"{ra['label']:<34} {ca} {cia}   {cb_} {cib}  {verdict}")

print()
print("=" * 100)
if winners:
    print("КАНДИДАТЫ В ПРОД (лучше на обеих БД и значимо хотя бы на одной):")
    for k, lab, da, db_, sa, sb in sorted(winners, key=lambda w: -(w[2] + w[3])):
        print(f"  {lab:<34} {LA} {da:+.2f}{'*' if sa else ' '}   {LB} {db_:+.2f}{'*' if sb else ' '}")
else:
    print("КАНДИДАТОВ НЕТ: ни одна конфигурация не выиграла значимо на обеих базах.")
    print("Это законный результат — менять конфиг не на чем.")
