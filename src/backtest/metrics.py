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
  * winrate — отчётная величина, целевой считается только вместе с E[R];
  * безубыток — ОТДЕЛЬНЫЙ класс исхода (`outcome`), а не «плюс»: сделку, снятую
    с безубыточного стопа, market вернул к цене входа, и считать её успешной
    неверно. В знаменателе winrate безубытки при этом остаются — см.
    `outcome_counts`.

Модуль намеренно не имеет зависимостей кроме stdlib: его импортирует не только
бэктест, но и боевая отчётность (`storage/stats.py` для /stats в Telegram),
чтобы классификация исхода не разъехалась между отчётами двумя копиями.
"""

import random
import statistics as st

# Фиксированное зерно: доверительный интервал не должен «дышать» между
# прогонами одной и той же конфигурации, иначе золотой тест движка станет
# флаки, а сравнение двух свипов — невоспроизводимым.
BOOTSTRAP_SEED = 20260922
BOOTSTRAP_RESAMPLES = 10_000


WIN, BREAKEVEN, LOSS = "win", "breakeven", "loss"

# Полуширина полосы «вышли примерно там, где вошли», % от цены входа.
#
# Полоса не подгонная, она зажата с двух сторон. Снизу — фактическим разбросом
# исполнения безубыточного стопа и комиссией: замер 42 стоповых и 38
# безубыточных выходов на боевой БД 27.08-22.09.2026 дал отклонение выхода в
# б/у медианно −0.003%, максимум 0.219%, а круговая комиссия в ценовом
# выражении около 0.11%. Сверху — ближайшими настоящими исходами: триггер
# партиала стоит на +3.5% от входа, стоп на −5%. 0.5% даёт семикратный запас в
# обе стороны, поэтому полоса не может проглотить ни настоящую прибыль, ни
# настоящий убыток.
BREAKEVEN_BAND_PCT = 0.5


def outcome(trade, band_pct: float = BREAKEVEN_BAND_PCT) -> str:
    """Исход сделки: `win` / `breakeven` / `loss`.

    Безубыток — отдельная категория, а не «плюс». Сделка, которую сняли с
    безубыточного стопа после частичной фиксации, приносит только
    забронированную на триггере часть: при доле 20%, RR 2.0 и триггере на 35%
    пути до TP это +0.14R против +2R у полного тейка. Считать её успешной
    неверно: рынок вернулся к цене входа, и сетап не отработал.

    Классификация по ЦЕНЕ ВЫХОДА относительно входа, а не по механизму и не по
    знаку PnL. Почему так:

    * по знаку PnL безубыток попадает в плюс, ради чего эта функция и появилась;
    * по механизму («был партиал и вышли по стопу») категория становится
      управляемой через `partial_close_qty_pct`: подняв долю, можно двигать
      winrate, не меняя денег. По цене выхода доля не влияет ни на что — она
      меняет только РАЗМЕР результата, а не его класс;
    * выход по времени, случайно оказавшийся у цены входа, — тоже безубыток по
      существу, и цена его так и классифицирует.

    Требует `entry_price` и `exit_price`. Их нет (частичные данные, старые
    прогоны) — остаётся двухклассовый ответ по знаку PnL, чтобы форма отчёта не
    ломалась.
    """
    entry = trade.get("entry_price")
    exit_price = trade.get("exit_price")
    pnl = trade.get("pnl") or 0.0
    if not entry or not exit_price:
        return WIN if pnl > 0 else LOSS
    # Сравнение в ЦЕНЕ, а не в процентах: пересчёт в проценты через деление
    # сдвигает границу на ошибку округления — (99.5/100 - 1) * 100 даёт
    # -0.5000000000000004, и цена ровно на краю полосы выпадала из безубытка.
    move = exit_price - entry
    if (trade.get("direction") or "long") == "short":
        move = -move
    if abs(move) <= abs(entry) * band_pct / 100:
        return BREAKEVEN
    return WIN if move > 0 else LOSS


# Допуск на попадание в тейк: TP выставлен лимитным ордером, и цена закрытия
# совпадает с ним с точностью до шага цены инструмента.
FULL_TAKE_TOLERANCE_PCT = 0.1


def is_full_take(trade, tolerance_pct: float = FULL_TAKE_TOLERANCE_PCT) -> bool | None:
    """Дошла ли сделка до полного тейка. None — `tp_price` неизвестна.

    Отдельно от `outcome`, потому что это разные вопросы. `outcome` отвечает
    «вышли выше или ниже входа», а полный тейк — «сетап отработал до конца».
    Между ними лежат выходы по времени: замер 26 суток дал 4 сделки из 108,
    вышедшие в плюс, но не доехавшие до TP, и 5 — в минус, но не доехавшие до
    стопа.

    Зачем нужна отдельная величина. Доля полных тейков — единственная отчётная
    метрика проекта, которая (а) не управляема `partial_close_qty_pct`: поднять
    долю партиала полных тейков не создаёт; (б) линейно связана с деньгами при
    фиксированном RR. Замер 23.09.2026: при RR 2.0 и доле 20% один полный тейк
    вместо стопа стоит +2.7R, то есть один лишний тейк на 108 сделок ≈ +2.7% к
    депозиту в месяц при риске 1%. Вся дистанция от убыточности системы (18.9%
    тейков) до +30%/мес (27.8%) — девять процентных пунктов.

    ВНИМАНИЕ: порог доли тейков имеет смысл только при фиксированном RR. Дальний
    TP делает тейки реже, но дороже, поэтому пороги из docs/backtest.md заданы
    для RR 2.0 и при его изменении должны пересчитываться.
    """
    tp = trade.get("tp_price")
    entry = trade.get("entry_price")
    exit_price = trade.get("exit_price")
    if not tp or not exit_price or not entry:
        return None
    if (trade.get("direction") or "long") == "short":
        return exit_price <= tp * (1 + tolerance_pct / 100)
    return exit_price >= tp * (1 - tolerance_pct / 100)


def breakeven_credit(risk_reward_ratio: float, partial_close_pct: float,
                     partial_close_qty_pct: float) -> float:
    """Какую долю полного тейка приносит безубыток. 0.0 — партиала нет вовсе.

    Безубыток отдаёт только часть, забронированную на триггере:
    `доля × RR × порог/100` в единицах R. Полный тейк отдаёт её же плюс остаток
    позиции по TP. При текущем конфиге (доля 20%, RR 2.0, триггер 35% пути до TP)
    это 0.14R против 1.74R, то есть **один полный тейк = 12.4 безубытка**.
    Замер нетто на 26 сутках (27.08-22.09.2026) дал +0.12R против +1.72R, то есть
    14.3 — расхождение целиком комиссионное, и формула даёт верхнюю оценку
    вклада примерно на 15% относительно.

    Величина считается по ТЕКУЩЕМУ конфигу, поэтому отчёт за период, внутри
    которого конфиг менялся, приблизителен: на доле партиала 30% (до 22.09.2026)
    вес был 0.130, то есть 7.7 безубытка на тейк. Точнее без хранения конфига у
    каждой сделки не получится.
    """
    q = partial_close_qty_pct / 100
    banked = q * risk_reward_ratio * partial_close_pct / 100
    full = banked + (1 - q) * risk_reward_ratio
    return banked / full if full else 0.0


def outcome_counts(trades, band_pct: float = BREAKEVEN_BAND_PCT,
                   be_credit: float | None = None) -> dict:
    """Три счётчика исходов плюс доли. Знаменатель всех долей — ВСЕ сделки.

    Безубыток из знаменателя не выносится. Вынести — значит снова сделать
    winrate управляемым: доля партиала определяет, сколько сделок окажется в
    безубытке, и «winrate без безубытков» поехал бы вместе с ней при тех же
    деньгах. Три числа рядом честнее одного, а решение всё равно принимается по
    E[R] с интервалом (AGENTS.md, правило 8).
    """
    n = len(trades)
    c = {WIN: 0, BREAKEVEN: 0, LOSS: 0}
    be_profitable = 0
    for t in trades:
        o = outcome(t, band_pct)
        c[o] += 1
        if o == BREAKEVEN and (t.get("pnl") or 0.0) > 0:
            be_profitable += 1
    takes = [is_full_take(t) for t in trades]
    known = [x for x in takes if x is not None]
    out = {
        "wins": c[WIN], "breakevens": c[BREAKEVEN], "losses": c[LOSS],
        "win_rate": round(100 * c[WIN] / n, 1) if n else None,
        "breakeven_rate": round(100 * c[BREAKEVEN] / n, 1) if n else None,
        "loss_rate": round(100 * c[LOSS] / n, 1) if n else None,
        # Доля полных тейков — целевая отчётная величина проекта, см.
        # `is_full_take` и docs/backtest.md. None, если `tp_price` неизвестна.
        "full_takes": sum(known) if known else None,
        "full_take_rate": round(100 * sum(known) / n, 1) if known and n else None,
    }
    if be_credit is not None:
        # Безубыток засчитывается в победы ПРОПОРЦИОНАЛЬНО прибыли от полного
        # тейка: 12.4 безубытка = один тейк при текущем конфиге. Кредит дают
        # только безубытки с положительным PnL — у остальных комиссия съела
        # забронированную часть, и зачитывать там нечего.
        equiv = be_profitable * be_credit
        out["breakeven_credit"] = round(be_credit, 4)
        out["breakeven_equivalent_wins"] = round(equiv, 2)
        out["win_rate_weighted"] = round(100 * (c[WIN] + equiv) / n, 1) if n else None
    return out


def bootstrap_ci(values, confidence: float = 0.95, resamples: int = BOOTSTRAP_RESAMPLES,
                 blocks=None):
    """Перцентильный bootstrap-интервал для СРЕДНЕГО значения.

    Именно bootstrap, а не t-интервал: распределение R трёхмодально (стоп -1,
    безубыток около +0.25, полный TP +2R) и на нормальность не похоже.

    `blocks` — параллельный `values` список ключей (обычно дата входа). Задан —
    ресэмплятся БЛОКИ ЦЕЛИКОМ, а не отдельные сделки.

    Почему это обязательно. Сделки не независимы: одно рыночное движение даёт
    сразу несколько сигналов по разным монетам в один и тот же момент. Замер
    22.09.2026: из разницы бэктеста с реалом в +15.26R **54% пришлось на один
    час** — девять сделок 16.09 18:00, то есть одно событие, а не девять
    наблюдений. Обычный bootstrap по сделкам дал [+0.48; +30.10] («значимо»),
    блочный по суткам — [-1.49; +36.32] («не значимо»). Первый ответ был
    ложноположительным.
    """
    n = len(values)
    if n < 2:
        # Одна сделка — интервала нет. Именно None, а не (nan, nan): NaN не
        # равен сам себе, и сравнение двух прогонов на такой выборке падало бы
        # там, где результат на самом деле идентичен.
        return None
    rnd = random.Random(BOOTSTRAP_SEED)

    if blocks is None:
        means = sorted(st.fmean(rnd.choices(values, k=n)) for _ in range(resamples))
    else:
        grouped: dict = {}
        for key, value in zip(blocks, values):
            grouped.setdefault(key, []).append(value)
        keys = sorted(grouped)
        if len(keys) < 2:
            return None
        # Среднее на СДЕЛКУ, а не на блок: блоки разного размера, и мы хотим
        # интервал для той же величины, что и точечная оценка.
        means = []
        for _ in range(resamples):
            picked = [v for k in rnd.choices(keys, k=len(keys)) for v in grouped[k]]
            means.append(st.fmean(picked))
        means.sort()
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


def summarize(trades, days: float | None = None, deposit: float | None = None,
              be_credit: float | None = None):
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
            "wins": 0, "breakevens": 0, "losses": 0,
            "breakeven_rate": None, "loss_rate": None,
            "full_takes": 0, "full_take_rate": None,
            **({"breakeven_credit": round(be_credit, 4),
                "breakeven_equivalent_wins": 0.0,
                "win_rate_weighted": None} if be_credit is not None else {}),
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

    scored = [t for t in trades if t.get("risk")]
    rs = [t["pnl"] / t["risk"] for t in scored]
    # Блок = сутки входа: сделки одного дня тянутся за общим движением рынка.
    blocks = [str(t.get("entry_time") or "")[:10] for t in scored]
    pnls = [t["pnl"] for t in trades]
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)
    ci = bootstrap_ci(rs, blocks=blocks) if rs else None

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
        **outcome_counts(trades, be_credit=be_credit),
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
    # Блок = сутки входа, см. bootstrap_ci: одно движение рынка даёт несколько
    # сигналов разом, и считать их независимыми — ложноположительный ответ.
    ci_mean = bootstrap_ci(diffs, blocks=[k[1][:10] for k in keys])
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
