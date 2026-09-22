"""
Свип порога ликвидности `strategy.min_baseline_volume_usdt`.

Вопрос (09.09.2026): порог подняли 5000 → 25000 аудитом 25.08.2026, чтобы убрать
сигналы по неликвиду. Но стратегия ловит в том числе пампы «тонких» монет, и на
25k торговля рискует сузиться до верхушки по обороту. Ищем компромисс на сетке
5000–25000 (плюс якоря 0 и 35000, чтобы видеть форму кривой, а не только её кусок).

Метрика решения — R/день ПОРТФЕЛЯ при `max_positions=10`, а не мат. ожидание на
сигнал: снижение порога увеличивает поток сигналов, и слоты начинают конкурировать
между собой. `simulate()` слоты моделирует, поэтому R/день сопоставим между
конфигурациями напрямую.

Движок цикла симуляции НЕ дублируется: зовём `src.backtest.engine.simulate`.
Порог живёт внутри `SetupDetector.check_volume_pattern`, которую движок вызывает,
так что своей копии гейта здесь нет и быть не должно (см. docs/backtest.md).

Каждая сделка аннотируется медианой baseline-объёма в USDT на момент входа
(окно детектора: 84 бара, baseline — первые 70). Это описательная величина для
бакет-анализа «сколько зарабатывают монеты каждой ступени ликвидности», в решениях
о сигналах она не участвует.

Использование:
    .venv/bin/python scripts/sweep_min_baseline_volume.py --db data/trading_bot.db \
        --out-dir /tmp/sweep_minvol
"""

import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.engine import DEFAULT_MARKETS_PATH, load_data, load_markets, log, simulate  # noqa: E402
from src.backtest.metrics import SIGNIFICANCE_LEGEND, fmt, format_delta  # noqa: E402
from src.config import Settings  # noqa: E402

THRESHOLDS = [0, 5000, 7500, 10000, 12500, 15000, 20000, 25000, 35000]


def annotate_baseline_usdt(trades: list[dict], data, baseline_bars: int, need_bars: int) -> None:
    """Медиана объёма × медиана цены по baseline-части окна детектора на входе.

    Окно движка — бары [i-need_bars-9 .. i], baseline — первые baseline_bars из них.
    При shift-ретрае вход мог случиться на баре i-1; для бакетов сдвиг на один бар
    несуществен (медиана по 70 барам), поэтому берём бар входа как есть.
    """
    sym_ts_to_idx = data["sym_ts_to_idx"]
    symbols = data["symbols"]
    for t in trades:
        t["baseline_usdt"] = None
        entry_ts = datetime.fromisoformat(t["entry_time"])
        idx = sym_ts_to_idx.get(t["symbol"], {}).get(entry_ts)
        if idx is None:
            continue
        start = idx - need_bars - 10 + 1
        if start < 0:
            continue
        base_bars = symbols[t["symbol"]][start:start + baseline_bars]
        if len(base_bars) < baseline_bars:
            continue
        med_vol = statistics.median(b[5] for b in base_bars)
        med_price = statistics.median(b[4] for b in base_bars)
        t["baseline_usdt"] = round(med_vol * med_price, 1)


def half_split(trades: list[dict], mid_iso: str) -> tuple[float, float, int, int]:
    """Сумма R в первой и второй половине периода (по времени ВХОДА).

    Риск берётся из самой сделки, а не общей константой: его двигают Circuit
    Breaker и множитель рыночного режима, и сделка половинного размера не
    должна весить как полноразмерная.
    """
    r1 = r2 = 0.0
    n1 = n2 = 0
    for t in trades:
        r = t["pnl"] / t["risk"] if t.get("risk") else 0.0
        if t["entry_time"] < mid_iso:
            r1 += r
            n1 += 1
        else:
            r2 += r
            n2 += 1
    return round(r1, 2), round(r2, 2), n1, n2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--has-oi", type=int, default=1)
    ap.add_argument("--thresholds", default="")
    ap.add_argument(
        "--markets", default=DEFAULT_MARKETS_PATH,
        help="метаданные инструментов для модели шага лота; пустая строка — выключить",
    )
    args = ap.parse_args()
    markets = load_markets(args.markets or None)

    thresholds = (
        [float(x) for x in args.thresholds.split(",")] if args.thresholds else THRESHOLDS
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_settings = Settings.from_yaml(args.config)
    baseline_bars = base_settings.strategy.baseline_bars
    need_bars = baseline_bars + base_settings.strategy.sustain_bars

    t0 = time.time()
    log(f"Loading {args.db} ...")
    data = load_data(args.db)
    # OI-гейт выключен в проде с 25.08.2026 — движок к oi_cache не обращается,
    # а держать его в памяти мешает гонять свип в несколько процессов сразу.
    # Освобождаем ТОЛЬКО когда гейт действительно выключен: иначе это молча
    # изменило бы результат (ровно тот класс parity-багов, см. docs/backtest.md).
    if not base_settings.strategy.oi_filter_enabled:
        data["oi_cache"] = {}
        log("oi_filter_enabled=false → oi_cache освобождён (движком не читается)")
    ts_all = data["all_timestamps"]
    days = (ts_all[-1] - ts_all[0]).total_seconds() / 86400
    mid_iso = ts_all[len(ts_all) // 2].isoformat()
    log(f"Data loaded in {time.time() - t0:.1f}s; период {days:.1f} дн; "
        f"{len(thresholds)} конфигураций; середина периода {mid_iso}")

    rows = []
    runs: dict[float, list[dict]] = {}
    for th in thresholds:
        t1 = time.time()
        s = Settings.from_yaml(args.config)
        s.strategy.min_baseline_volume_usdt = th
        r = simulate(s, data, markets=markets, has_oi=bool(args.has_oi), collect_retracement=False)
        annotate_baseline_usdt(r["trades_list"], data, baseline_bars, need_bars)
        runs[th] = r["trades_list"]
        total_r = r["total_R"]
        r1, r2, n1, n2 = half_split(r["trades_list"], mid_iso)
        liq = [t["baseline_usdt"] for t in r["trades_list"] if t["baseline_usdt"]]
        row = {
            "threshold": th,
            "signals": r["signals"],
            "trades": r["trades"],
            "win_rate": r["win_rate"],
            "tp": r["tp_wins"], "sl": r["sl_losses"], "time": r["time_exits"],
            "total_pnl": r["total_pnl"],
            "total_R": total_r,
            "R_per_trade": r["expectancy_R"],
            "R_ci": r["expectancy_R_ci"],
            "R_per_day": r["R_per_day"],
            "symbols": len(set(t["symbol"] for t in r["trades_list"])),
            "median_liq_usdt": round(statistics.median(liq)) if liq else None,
            "R_half1": r1, "R_half2": r2, "n_half1": n1, "n_half2": n2,
            "max_drawdown_R": r["max_drawdown_R"],
            "worst_loss_streak": r["worst_loss_streak"],
        }
        rows.append(row)
        log(f"th={th:>7.0f}: сигналов={r['signals']:4d} сделок={r['trades']:3d} "
            f"WR={r['win_rate']:5.1f}% R={fmt(total_r):>7} "
            f"R/день={fmt(row['R_per_day'], '+.3f')} "
            f"R/сделку={fmt(row['R_per_trade'], '+.4f')} монет={row['symbols']:3d} "
            f"половины={r1:+.2f}/{r2:+.2f} просадка={row['max_drawdown_R']:.2f}R "
            f"({time.time() - t1:.0f}s)")
        with open(out_dir / f"minvol_{th:g}.json", "w") as f:
            json.dump(r, f, indent=2, default=str)

    # Сравнение с боевым порогом — парное: общие сделки дают ровно ноль и не
    # раздувают интервал. Решающая метрика здесь R/день ПОРТФЕЛЯ, а не R на
    # сигнал: при max_positions сигналы конкурируют за слоты, и сетап с меньшим
    # мат. ожиданием всё равно повышает отдачу, если занимает пустой слот.
    prod_th = base_settings.strategy.min_baseline_volume_usdt
    log("")
    if prod_th in runs:
        log(f"{'порог':>8} {'сделок':>7} {'R/день':>8} {'R/сделку':>10} "
            f"{'просадка':>9} {'половины':>16} {f'Δ R к {prod_th:g} (95% ДИ)':>30}")
        for row in rows:
            th = row["threshold"]
            delta = "— (боевой)"
            if th != prod_th:
                delta, row["vs_prod"] = format_delta(runs[th], runs[prod_th], days=days)
            log(f"{th:>8.0f} {row['trades']:>7} {fmt(row['R_per_day'], '+.3f'):>8} "
                f"{fmt(row['R_per_trade'], '+.4f'):>10} {row['max_drawdown_R']:>8.2f}R "
                f"{row['R_half1']:>+7.2f}/{row['R_half2']:>+7.2f} {delta:>30}")
        log("")
        log(SIGNIFICANCE_LEGEND)
        log("  Половины периода должны улучшаться обе, иначе это подгонка под отрезок.")

    with open(out_dir / "summary.json", "w") as f:
        json.dump({"db": args.db, "days": days, "prod_threshold": prod_th, "rows": rows},
                  f, indent=2, ensure_ascii=False)
    log(f"Готово за {time.time() - t0:.0f}s → {out_dir}/summary.json")


if __name__ == "__main__":
    main()
