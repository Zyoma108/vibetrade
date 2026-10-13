"""Свип ATR-стопа (13.10.2026): источник меры × множитель × масштаб трейлинга.

    .venv/bin/python scripts/sweep_atr_stop.py <db> <outdir> <tag>
    .venv/bin/python scripts/sweep_atr_stop.py pool <outfile> <sweep1.json> <sweep2.json> ...

Две стадии на одной загрузке БД (порядок — как в программе этапа 3):
  1. чистая модель — депозит $1000, шаг лота выключен (markets=None);
  2. реальный депозит $60, шаг лота вкл (config/bybit_markets.json).

Каждая конфигурация сравнивается с боевой (stop_loss_atr_mult = 0) ПАРНО
(`metrics.compare`): блочный bootstrap по суткам, звёздочка = интервал
разницы не накрывает ноль (правило 8 AGENTS.md). mult=0 обязан совпасть с
боевым конфигом посделочно — встроенная проверка parity.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.backtest.engine import load_data, load_markets, log, simulate  # noqa: E402
from src.backtest.metrics import compare  # noqa: E402
from src.config import Settings  # noqa: E402

# Сетки по источникам подобраны под распределение меры на реальных сделках
# (scripts/measure_atr_at_entries.py, 22.09-28.09 и 27.08-22.09):
# baseline ATR med 0.44-0.54% (p10-p90 ≈ 0.26-1.0), signal med 0.61-0.71%
# (p10-p90 ≈ 0.38-1.08). Множители покрывают стопы 2-4% по всему размаху.
# FIXED_REFS — контрольные фиксированные стопы: отделяют пользу АДАПТИВНОСТИ
# от пользы просто более тесного стопа (урок концепта «б» ревизии 23.09:
# «вывод не "привязать стоп к шуму", а "стоп просто широк"»).
GRIDS = {
    "baseline": (3.0, 4.0, 5.0, 6.0, 8.0),
    "signal": (2.5, 3.0, 4.0, 5.0, 6.0),
}
FIXED_REFS = (3.0, 3.5)
STAGES = ("clean", "deposit")


def _trades(r):
    return [
        {k: t[k] for k in ("symbol", "entry_time", "pnl", "risk", "exit_reason",
                           "stop_pct") if k in t}
        for t in r["trades_list"]
    ]


def run_db(db: str, outdir: str, tag: str, config: str = "config/config.yaml"):
    t0 = time.time()
    data = load_data(db)
    ts = data["all_timestamps"]
    days = (ts[-1] - ts[0]).total_seconds() / 86400
    markets = load_markets(None)
    log(f"[{tag}] загрузка {time.time()-t0:.0f}s; период {days:.1f} суток; "
        f"markets={'да' if markets else 'нет'}")

    out = {"db": db, "days": round(days, 2), "tag": tag, "stages": {}}

    for stage in STAGES:
        deposit = 1000.0 if stage == "clean" else 60.0
        mk = None if stage == "clean" else markets
        stage_out = {}

        base = Settings.from_yaml(config)
        base.trading.backtest_deposit_usdt = deposit
        r0 = simulate(base, data, has_oi=True, collect_retracement=False, markets=mk)
        prod_trades = _trades(r0)
        stage_out["prod"] = {
            "trades": prod_trades,
            "summary": {k: r0.get(k) for k in
                        ("trades", "signals", "total_pnl", "win_rate",
                         "full_take_rate", "max_drawdown_R", "total_R",
                         "amount_too_small")},
        }
        log(f"[{tag}/{stage}] prod: сделок={r0['trades']} "
            f"ΣR={r0.get('total_R', 0):+.2f}")

        for source, mults in GRIDS.items():
            for mult in mults:
                for scale in (False, True):
                    s = Settings.from_yaml(config)
                    s.trading.backtest_deposit_usdt = deposit
                    s.trading.stop_loss_atr_mult = mult
                    s.trading.stop_loss_atr_source = source
                    s.trading.trailing_scale_with_stop = scale
                    r = simulate(s, data, has_oi=True,
                                 collect_retracement=False, markets=mk)
                    label = f"{source[:4]}_x{mult:g}" + ("_ts" if scale else "")
                    cmp = compare(_trades(r), prod_trades, days=days)
                    stage_out[label] = {
                        "overrides": {
                            "trading.stop_loss_atr_mult": mult,
                            "trading.stop_loss_atr_source": source,
                            "trading.trailing_scale_with_stop": scale,
                        },
                        "trades": _trades(r),
                        "summary": {k: r.get(k) for k in
                                    ("trades", "total_pnl", "win_rate",
                                     "full_take_rate", "max_drawdown_R",
                                     "total_R", "amount_too_small")},
                        "compare_vs_prod": cmp,
                    }
                    log(f"[{tag}/{stage}] {label:<14} сделок={r['trades']:3d} "
                        f"ΣR={r.get('total_R', 0):+7.2f} "
                        f"Δ={cmp['delta_R']:+7.2f} {cmp['delta_R_ci']}")

        for fixed in FIXED_REFS:
            s = Settings.from_yaml(config)
            s.trading.backtest_deposit_usdt = deposit
            s.trading.stop_loss_pct = fixed
            r = simulate(s, data, has_oi=True,
                         collect_retracement=False, markets=mk)
            cmp = compare(_trades(r), prod_trades, days=days)
            label = f"fixed{fixed:g}"
            stage_out[label] = {
                "overrides": {"trading.stop_loss_pct": fixed},
                "trades": _trades(r),
                "summary": {k: r.get(k) for k in
                            ("trades", "total_pnl", "win_rate",
                             "full_take_rate", "max_drawdown_R",
                             "total_R", "amount_too_small")},
                "compare_vs_prod": cmp,
            }
            log(f"[{tag}/{stage}] {label:<14} сделок={r['trades']:3d} "
                f"ΣR={r.get('total_R', 0):+7.2f} "
                f"Δ={cmp['delta_R']:+7.2f} {cmp['delta_R_ci']}")

        out["stages"][stage] = stage_out

    path = Path(outdir) / f"sweep_{tag}.json"
    json.dump(out, open(path, "w"), ensure_ascii=False)
    log(f"[{tag}] готово за {(time.time()-t0)/60:.0f} мин -> {path}")


def pool(outfile: str, inputs: list[str]):
    """Объединённая оценка по суточным блокам всех БД (как sweep_pool этапа 3).

    Для каждой конфигурации: разницы R к prod по ВСЕМ сделкам всех БД (общие
    дают ноль), блочный bootstrap по суткам ВНУТРИ каждой БД, блоки склеены.
    """
    import numpy as np

    from src.backtest.metrics import bootstrap_ci

    sweeps = [json.load(open(p)) for p in inputs]
    labels: dict[str, dict] = {}
    for sw in sweeps:
        for stage in STAGES:
            prod = sw["stages"][stage]["prod"]
            pk = {(t["symbol"], t["entry_time"]): t["pnl"] / t["risk"]
                  for t in prod["trades"] if t.get("risk")}
            for label, cfg in sw["stages"][stage].items():
                if label == "prod":
                    continue
                ck = {(t["symbol"], t["entry_time"]): t["pnl"] / t["risk"]
                      for t in cfg["trades"] if t.get("risk")}
                for key in sorted(set(pk) | set(ck)):
                    diff = ck.get(key, 0.0) - pk.get(key, 0.0)
                    block = f"{sw['tag']}|{stage}|{key[1][:10]}"
                    labels.setdefault(f"{stage}|{label}", {}).setdefault(block, 0.0)
                    labels[f"{stage}|{label}"][block] += diff

    result = {}
    for name, blocks in sorted(labels.items()):
        diffs = list(blocks.values())
        ci = bootstrap_ci(diffs, blocks=list(blocks.keys()))
        total = sum(diffs)
        result[name] = {
            "delta_R": round(total, 2),
            "delta_R_ci": [round(ci[0] * len(diffs), 2),
                           round(ci[1] * len(diffs), 2)] if ci else None,
            "days": len(blocks),
        }
        result[name]["significant"] = bool(
            ci and (ci[0] > 0 or ci[1] < 0)
        )
        log(f"{name:<28} Δ={total:+8.2f} {result[name]['delta_R_ci']} "
            f"{'*' if result[name]['significant'] else ''}")
    json.dump(result, open(outfile, "w"), indent=1, ensure_ascii=False)
    log(f"-> {outfile}")


if __name__ == "__main__":
    if sys.argv[1] == "pool":
        pool(sys.argv[2], sys.argv[3:])
    else:
        run_db(sys.argv[1], sys.argv[2], sys.argv[3])
