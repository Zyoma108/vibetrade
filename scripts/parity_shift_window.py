"""Parity правки окна shift-ретрая: прогон движка и посделочное сравнение.

    # прогон — из корня ТОЙ ветки, чей код меряем (правило 3 AGENTS.md)
    cd /tmp/vt_old && /path/.venv/bin/python /path/scripts/parity_shift_window.py run DB OUT.json
    .venv/bin/python scripts/parity_shift_window.py run DB OUT.json
    # сравнение
    .venv/bin/python scripts/parity_shift_window.py compare BASE.json NEW.json

Импорт `src` идёт из ТЕКУЩЕГО каталога, а не из каталога скрипта: один и тот же
скрипт меряет и worktree ветки до правки, и рабочую копию после.

База открывается как `immutable`: архивные БД в `data/` лежат в WAL-режиме, и
обычное открытие пытается создать `-shm` рядом с файлом. Только чтение.

Две стадии (память sweep-two-stage-deposit-rule): чистая модель на $1000 без шага
лота и реальный депозит $60 с `config/bybit_markets.json`.
"""
import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.getcwd())

_connect = sqlite3.connect


def _immutable_connect(path, *args, **kwargs):
    if isinstance(path, str) and not path.startswith("file:"):
        return _connect(f"file:{os.path.abspath(path)}?immutable=1", *args, uri=True, **kwargs)
    return _connect(path, *args, **kwargs)


sqlite3.connect = _immutable_connect

STAGES = {
    "clean_1000": (1000.0, None),
    "lot_60": (60.0, "config/bybit_markets.json"),
}


def run(db: str, out_path: str) -> None:
    import src
    from src.backtest.engine import load_data, load_markets, log, simulate
    from src.config import Settings

    log(f"код: {os.path.dirname(src.__file__)}")

    t0 = time.time()
    data = load_data(db)
    days = (data["all_timestamps"][-1] - data["all_timestamps"][0]).total_seconds() / 86400
    log(f"{db}: загрузка {time.time() - t0:.0f}s, {days:.1f} сут")
    out = {"db": db, "days": days, "code": os.getcwd(), "stages": {}}
    for stage, (deposit, markets_path) in STAGES.items():
        s = Settings.from_yaml("config/config.yaml")
        # Риск на сделку на R не влияет, но влияет на шаг лота на малом депозите.
        # Фиксируем 1%, чтобы обе ветки мерились одинаково, какой бы риск ни
        # стоял в конфиге рабочей копии.
        s.trading.risk_per_trade_pct = 1.0
        s.trading.backtest_deposit_usdt = deposit
        markets = load_markets(markets_path) if markets_path else None
        r = simulate(s, data, has_oi=True, collect_retracement=False, markets=markets)
        out["stages"][stage] = {
            "trades": r["trades_list"],
            "total_R": r["total_R"], "expectancy_R": r["expectancy_R"],
            "full_take_rate": r["full_take_rate"], "R_per_day": r.get("R_per_day"),
            "shift_used_count": r["shift_used_count"], "signals": r["signals"],
        }
        log(f"  {stage}: сделок {r['trades']}, ΣR {r['total_R']:+.2f}, "
            f"сдвигов {r['shift_used_count']}, {time.time() - t0:.0f}s")
    with open(out_path, "w") as f:
        json.dump(out, f, ensure_ascii=False, default=str)


def compare(base_path: str, new_path: str) -> None:
    from src.backtest.metrics import compare as paired, summarize

    base, new = json.load(open(base_path)), json.load(open(new_path))
    days = new["days"]
    print(f"{new['db']}  ({days:.1f} сут)")
    for stage in STAGES:
        a, b = base["stages"][stage], new["stages"][stage]
        ka = {(t["symbol"], t["entry_time"]): t for t in a["trades"]}
        kb = {(t["symbol"], t["entry_time"]): t for t in b["trades"]}
        shared = set(ka) & set(kb)
        changed = [k for k in shared
                   if round(ka[k]["pnl"] / ka[k]["risk"], 9) != round(kb[k]["pnl"] / kb[k]["risk"], 9)]
        sa, sb = summarize(a["trades"], days=days), summarize(b["trades"], days=days)
        c = paired(b["trades"], a["trades"], days=days)
        print(f"\n  [{stage}] сдвигов движка {a['shift_used_count']} -> {b['shift_used_count']}")
        for label, s in (("до   ", sa), ("после", sb)):
            print(f"    {label}: сделок {s['trades']:3d}  ΣR {s['total_R']:+7.2f}  "
                  f"E[R] {s['expectancy_R']:+.3f} {s['expectancy_R_ci']}  "
                  f"тейков {(s['full_take_rate'] or 0):4.1f}%  R/сут {s['R_per_day']:+.3f}")
        print(f"    общих {len(shared)}, из них с другим R {len(changed)}; "
              f"только до {c['only_b']}, только после {c['only_a']}")
        print(f"    Δ после−до: {c['delta_R']:+.2f}R {c['delta_R_ci']}"
              f"{' *' if c['significant'] else ''}, {c.get('delta_R_per_day', 0):+.3f} R/сут")
        for k in sorted(set(ka) - set(kb), key=lambda x: x[1]):
            t = ka[k]
            print(f"      только до:    {k[1][:16]} {k[0]:<22} R {t['pnl'] / t['risk']:+.2f} {t['exit_reason']}")
        for k in sorted(set(kb) - set(ka), key=lambda x: x[1]):
            t = kb[k]
            print(f"      только после: {k[1][:16]} {k[0]:<22} R {t['pnl'] / t['risk']:+.2f} {t['exit_reason']}")
        for k in sorted(changed, key=lambda x: x[1]):
            print(f"      другой R:     {k[1][:16]} {k[0]:<22} "
                  f"{ka[k]['pnl'] / ka[k]['risk']:+.2f} -> {kb[k]['pnl'] / kb[k]['risk']:+.2f}")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "run":
        run(sys.argv[2], sys.argv[3])
    elif cmd == "compare":
        compare(sys.argv[2], sys.argv[3])
    else:
        sys.exit(f"неизвестная команда {cmd}")
