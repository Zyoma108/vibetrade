"""Метрики решения для бэктеста — единственное место, где они считаются.

Зачем отдельный модуль: до 22.09.2026 R-нормировка, просадка и худшая серия
жили копиями в `scripts/sweep_circuit_breaker.py` и
`scripts/sweep_min_baseline_volume.py`, а остальные свипы сравнивали конфигурации
по `total_pnl` без всякой оценки неопределённости. На выборках в 40-110 сделок
это и приводило к решениям, которые потом откатывались: разница в $2 между
конфигурациями выглядит убедительно ровно до тех пор, пока рядом нет
доверительного интервала шириной $8.

Соглашение по метрикам (22.09.2026):
  * решающая величина — E[R] с доверительным интервалом;
  * безубыточные сделки НЕ исключаются из статистики и не выносятся из
    знаменателя winrate: они честно весят своим вкладом в R. Исключение их
    делает winrate управляемым через partial_close_qty_pct, не меняя денег;
  * winrate — отчётная величина, целевой считается только вместе с E[R].
"""

import random
import statistics as st

# Фиксированное зерно: доверительный интервал не должен «дышать» между
# прогонами одной и той же конфигурации, иначе золотой тест движка станет
# флаки, а сравнение двух свипов — невоспроизводимым.
BOOTSTRAP_SEED = 20260922
BOOTSTRAP_RESAMPLES = 10_000


def bootstrap_ci(values, confidence: float = 0.95, resamples: int = BOOTSTRAP_RESAMPLES):
    """Перцентильный bootstrap-интервал для СРЕДНЕГО значения.

    Именно bootstrap, а не t-интервал: распределение R трёхмодально (стоп -1,
    безубыток около +0.25, полный TP +2R) и на нормальность не похоже.
    """
    n = len(values)
    if n < 2:
        # Одна сделка — интервала нет. Именно None, а не (nan, nan): NaN не
        # равен сам себе, и сравнение двух прогонов на такой выборке падало бы
        # там, где результат на самом деле идентичен.
        return None
    rnd = random.Random(BOOTSTRAP_SEED)
    pick = rnd.choices
    means = sorted(st.fmean(pick(values, k=n)) for _ in range(resamples))
    lo = (1 - confidence) / 2
    return (means[int(lo * resamples)], means[min(int((1 - lo) * resamples), resamples - 1)])


def equity_metrics(trades, risk_key: str = "risk"):
    """Просадка эквити и худшая серия убытков — по порядку ВЫХОДА из сделок.

    Порядок именно выхода, а не входа: просадку счёта создаёт момент фиксации
    убытка, а позиции живут до 72 часов и закрываются не в том порядке, в
    котором открывались.
    """
    ordered = sorted(trades, key=lambda t: t.get("exit_time") or "")
    eq = peak = max_dd = 0.0
    streak = worst_streak = 0
    for t in ordered:
        risk = t.get(risk_key) or 0.0
        eq += (t["pnl"] / risk) if risk else 0.0
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
        streak = streak + 1 if t["pnl"] <= 0 else 0
        worst_streak = max(worst_streak, streak)
    return {"max_drawdown_R": round(max_dd, 2), "worst_loss_streak": worst_streak}


def summarize(trades, days: float | None = None, deposit: float | None = None):
    """Полный набор метрик решения по списку закрытых сделок (`trades_list`).

    `trades` — словари из `engine.simulate()`; обязателен ключ `risk` (бюджет
    риска в долларах на момент ВХОДА). Без него R считать не из чего: делить
    PnL на `entry_price * quantity` нельзя, потому что `quantity` уменьшается
    при частичном закрытии и для таких сделок R завышается в
    1/(1-partial_close_qty_pct) раз — на доле 30% это ×1.43 у ~60% сделок.
    """
    if not trades:
        # Форма ответа одна и та же со сделками и без: потребитель (отчёт, свип)
        # не должен проверять наличие каждого ключа. Пустой прогон — обычный
        # исход, например когда фильтр отсёк всё.
        #
        # None там, где величина НЕ ОПРЕДЕЛЕНА (мат. ожидание, profit factor,
        # winrate), и 0.0 там, где она определена и равна нулю (сумма R,
        # просадка). Ноль вместо прочерка читался бы как измеренное значение.
        empty = {
            "trades": 0, "expectancy_R": None, "expectancy_R_ci": None,
            "expectancy_R_significant": False, "total_R": 0.0, "sd_R": None,
            "profit_factor": None, "win_rate": None, "total_pnl": 0.0,
            "max_drawdown_R": 0.0, "worst_loss_streak": 0,
            "hold_hours_median": None, "hold_hours_p90": None,
        }
        if days:
            empty["trades_per_day"] = 0.0
            empty["R_per_day"] = 0.0
        if deposit:
            empty["return_pct_of_deposit"] = 0.0
            if days:
                empty["return_pct_per_30d"] = 0.0
        return empty

    rs = [t["pnl"] / t["risk"] for t in trades if t.get("risk")]
    pnls = [t["pnl"] for t in trades]
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)
    ci = bootstrap_ci(rs) if rs else None

    holds = []
    for t in trades:
        if t.get("entry_time") and t.get("exit_time"):
            from datetime import datetime
            holds.append(
                (datetime.fromisoformat(t["exit_time"])
                 - datetime.fromisoformat(t["entry_time"])).total_seconds() / 3600
            )

    out = {
        "trades": len(trades),
        "expectancy_R": round(st.fmean(rs), 4) if rs else None,
        "expectancy_R_ci": (round(ci[0], 4), round(ci[1], 4)) if ci else None,
        # Знак интервала — это и есть критерий «есть эффект или показалось».
        "expectancy_R_significant": bool(ci) and (ci[0] > 0 or ci[1] < 0),
        "total_R": round(sum(rs), 2) if rs else None,
        "sd_R": round(st.pstdev(rs), 3) if len(rs) > 1 else None,
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss else None,
        "win_rate": round(100 * sum(1 for p in pnls if p > 0) / len(pnls), 1),
        "total_pnl": round(sum(pnls), 2),
        **equity_metrics(trades),
    }
    if holds:
        holds.sort()
        out["hold_hours_median"] = round(st.median(holds), 1)
        out["hold_hours_p90"] = round(holds[int(0.9 * len(holds))], 1)
    if days:
        out["trades_per_day"] = round(len(trades) / days, 2)
        out["R_per_day"] = round(sum(rs) / days, 3) if rs else None
    if deposit:
        # Без компаундинга: сумма PnL к исходному депозиту. Компаундинг —
        # отдельная величина, см. engine (equity-режим).
        out["return_pct_of_deposit"] = round(100 * sum(pnls) / deposit, 2)
        if days:
            out["return_pct_per_30d"] = round(100 * sum(pnls) / deposit * 30 / days, 2)
    return out


def fmt(value, spec: str = "+.2f", dash: str = "—") -> str:
    """Форматирование метрики, которой может не быть.

    Конфигурация без единой сделки — законный исход свипа (порог отсёк всё),
    и печать не должна на нём падать. При этом «нет данных» показывается
    прочерком, а не нулём: ноль читается как измеренное значение.
    """
    return dash if value is None else format(value, spec)


def _trade_key(t) -> tuple:
    """Идентификатор сделки для сопоставления двух прогонов."""
    return (t["symbol"], t["entry_time"])


def compare(trades_a, trades_b, days: float | None = None):
    """Парное сравнение двух конфигураций: A минус B, с интервалом на РАЗНИЦУ.

    Зачем парно. Сравнивать два прогона по отдельным суммам R — значит
    складывать два независимых шума там, где большинство сделок у конфигураций
    общие и дают ровно одинаковый вклад. Разница по общим сделкам равна нулю
    и не должна попадать в оценку неопределённости; работает только то, чем
    конфигурации реально отличаются. Непарное сравнение раздувает интервал и
    одновременно позволяет принять за эффект разницу в пару долларов — именно
    так в истории проекта принимались решения, которые потом откатывались.

    Сделка, которую взяла только одна конфигурация, входит в разницу целиком:
    её R против нуля. Это и есть цена решения «брать или не брать».

    ЧЕГО ЭТОТ ИНТЕРВАЛ НЕ УЧИТЫВАЕТ: bootstrap считает сделки независимыми.
    Они не независимы — позиции пересекаются во времени и тянутся за общим
    режимом рынка. Поэтому настоящая неопределённость ШИРЕ полученной, и
    интервал, едва не накрывающий ноль, доверия не заслуживает.
    """
    ra = {_trade_key(t): t["pnl"] / t["risk"] for t in trades_a if t.get("risk")}
    rb = {_trade_key(t): t["pnl"] / t["risk"] for t in trades_b if t.get("risk")}
    keys = sorted(set(ra) | set(rb))
    if not keys:
        return {"shared": 0, "only_a": 0, "only_b": 0, "delta_R": 0.0,
                "delta_R_ci": None, "significant": False}

    diffs = [ra.get(k, 0.0) - rb.get(k, 0.0) for k in keys]
    ci_mean = bootstrap_ci(diffs)
    n = len(diffs)
    out = {
        "shared": len(set(ra) & set(rb)),
        "only_a": len(set(ra) - set(rb)),
        "only_b": len(set(rb) - set(ra)),
        "delta_R": round(sum(diffs), 2),
        # Интервал на СУММУ разницы: n × интервал на среднюю разницу.
        "delta_R_ci": (round(ci_mean[0] * n, 2), round(ci_mean[1] * n, 2)) if ci_mean else None,
    }
    out["significant"] = bool(
        out["delta_R_ci"] and (out["delta_R_ci"][0] > 0 or out["delta_R_ci"][1] < 0)
    )
    if days:
        out["delta_R_per_day"] = round(sum(diffs) / days, 3)
    return out


def format_delta(trades_a, trades_b, days: float | None = None) -> tuple[str, dict]:
    """Готовая ячейка «Δ R к базе (95% ДИ)» и сам результат сравнения.

    Формат один на все свипы намеренно: звёздочка means интервал не накрывает
    ноль. Без звёздочки решение принимать не на чем, какой бы убедительной ни
    выглядела сама величина — это и есть главный урок откаченных экспериментов.
    """
    c = compare(trades_a, trades_b, days=days)
    ci = c["delta_R_ci"]
    mark = "*" if c["significant"] else " "
    cell = (f"{c['delta_R']:+.2f}{mark} [{ci[0]:+.2f}; {ci[1]:+.2f}]"
            if ci else f"{c['delta_R']:+.2f}")
    return cell, c


SIGNIFICANCE_LEGEND = (
    "* — интервал разницы не накрывает ноль. Без звёздочки эффекта нет,\n"
    "  какой бы убедительной ни выглядела сама величина."
)


def sweep_table(rows, runs, prod_key, key_header: str, days=None, emit=print) -> None:
    """Итоговая таблица свипа: метрики каждой конфигурации + парная разница.

    Один формат на все свипы намеренно. До 22.09.2026 каждый скрипт печатал
    своё, сравнивал конфигурации по `total_pnl` и не показывал неопределённость
    вовсе — отсюда и решения, которые потом откатывались.

    `rows` — словари со свободным ключом `key` и метриками из `simulate()`;
    `runs` — соответствие key → `trades_list`; `prod_key` — боевая конфигурация,
    относительно которой считается разница.
    """
    emit("")
    emit(f"{key_header:>12} {'сделок':>7} {'WR':>6} {'сумма R':>9} {'R/сделку':>10} "
         f"{'Δ R к боевому (95% ДИ)':>30}")
    for row in rows:
        key = row["key"]
        if key == prod_key:
            delta = "— (боевой)"
        elif prod_key in runs:
            delta, row["vs_prod"] = format_delta(runs[key], runs[prod_key], days=days)
        else:
            delta = "—"
        wr = row.get("win_rate")
        emit(f"{row['label']:>12} {row['trades']:>7} "
             f"{(fmt(wr, '.1f') + '%' if wr is not None else '—'):>6} "
             f"{fmt(row['total_R'], '+.2f'):>9} {fmt(row['R_per_trade'], '+.4f'):>10} "
             f"{delta:>30}")
    emit("")
    emit(SIGNIFICANCE_LEGEND)
