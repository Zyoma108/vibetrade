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

from src.backtest.engine import load_data, log, simulate  # noqa: E402
from src.config import Settings  # noqa: E402

VIRTUAL_BALANCE = 1000.0

THRESHOLDS = [0, 5000, 7500, 10000, 12500, 15000, 20000, 25000, 35000]


def equity_metrics(trades: list[dict], risk: float) -> dict:
    """Просадка и серии по эквити, упорядоченной временем ВЫХОДА из сделки."""
    ordered = sorted(trades, key=lambda t: t["exit_time"] or "")
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    streak = 0
    worst_streak = 0
    for t in ordered:
        eq += t["pnl"] / risk
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
        if t["pnl"] <= 0:
            streak += 1
            worst_streak = max(worst_streak, streak)
        else:
            streak = 0
    return {"max_drawdown_R": round(max_dd, 2), "worst_loss_streak": worst_streak}


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


def half_split(trades: list[dict], risk: float, mid_iso: str) -> tuple[float, float, int, int]:
    """Сумма R в первой и второй половине периода (по времени ВХОДА)."""
    r1 = r2 = 0.0
    n1 = n2 = 0
    for t in trades:
        if t["entry_time"] < mid_iso:
            r1 += t["pnl"] / risk
            n1 += 1
        else:
            r2 += t["pnl"] / risk
            n2 += 1
    return round(r1, 2), round(r2, 2), n1, n2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/trading_bot.db")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--has-oi", type=int, default=1)
    ap.add_argument("--thresholds", default="")
    args = ap.parse_args()

    thresholds = (
        [float(x) for x in args.thresholds.split(",")] if args.thresholds else THRESHOLDS
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_settings = Settings.from_yaml(args.config)
    risk = VIRTUAL_BALANCE * (base_settings.trading.risk_per_trade_pct / 100)
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
    for th in thresholds:
        t1 = time.time()
        s = Settings.from_yaml(args.config)
        s.strategy.min_baseline_volume_usdt = th
        r = simulate(s, data, has_oi=bool(args.has_oi), collect_retracement=False)
        annotate_baseline_usdt(r["trades_list"], data, baseline_bars, need_bars)
        total_r = r["total_pnl"] / risk
        r1, r2, n1, n2 = half_split(r["trades_list"], risk, mid_iso)
        liq = [t["baseline_usdt"] for t in r["trades_list"] if t["baseline_usdt"]]
        row = {
            "threshold": th,
            "signals": r["signals"],
            "trades": r["trades"],
            "win_rate": r["win_rate"],
            "tp": r["tp_wins"], "sl": r["sl_losses"], "time": r["time_exits"],
            "total_pnl": r["total_pnl"],
            "total_R": round(total_r, 2),
            "R_per_trade": round(total_r / r["trades"], 4) if r["trades"] else 0.0,
            "R_per_day": round(total_r / days, 3),
            "symbols": len(set(t["symbol"] for t in r["trades_list"])),
            "median_liq_usdt": round(statistics.median(liq)) if liq else None,
            "R_half1": r1, "R_half2": r2, "n_half1": n1, "n_half2": n2,
            **equity_metrics(r["trades_list"], risk),
        }
        rows.append(row)
        log(f"th={th:>7.0f}: сигналов={r['signals']:4d} сделок={r['trades']:3d} "
            f"WR={r['win_rate']:5.1f}% R={total_r:+7.2f} R/день={row['R_per_day']:+.3f} "
            f"R/сделку={row['R_per_trade']:+.4f} монет={row['symbols']:3d} "
            f"половины={r1:+.2f}/{r2:+.2f} просадка={row['max_drawdown_R']:.2f}R "
            f"({time.time() - t1:.0f}s)")
        with open(out_dir / f"minvol_{th:g}.json", "w") as f:
            json.dump(r, f, indent=2, default=str)

    with open(out_dir / "summary.json", "w") as f:
        json.dump({"db": args.db, "days": days, "rows": rows}, f, indent=2)
    log(f"Готово за {time.time() - t0:.0f}s → {out_dir}/summary.json")


if __name__ == "__main__":
    main()
