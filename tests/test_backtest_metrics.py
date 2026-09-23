"""Метрики решения: bootstrap-интервал, R-нормировка, просадка.

Зачем отдельный файл: до 22.09.2026 свипы сравнивали конфигурации по total_pnl
без оценки неопределённости, а R-нормировка и просадка жили копиями в двух
скриптах. Решения по разнице в пару долларов на выборке 40-110 сделок
принимались и потом откатывались — см. историю в docs/backtest.md.
"""

import pytest

from src.backtest.metrics import (
    bootstrap_ci,
    compare,
    equity_metrics,
    fmt,
    summarize,
)


_N = 0


def _t(pnl: float, risk: float = 10.0, hour: int = 0, day: int | None = None) -> dict:
    """Сделка для тестов. День по умолчанию разный у каждой следующей.

    Это существенно, а не косметика: доверительный интервал считается блочным
    bootstrap'ом по суткам входа, и выборка, целиком уместившаяся в один день,
    честно считается одним наблюдением — какой бы большой она ни была.
    """
    global _N
    if day is None:
        _N += 1
        day = _N % 25 + 1
    return {
        "pnl": pnl,
        "risk": risk,
        "entry_time": f"2026-09-{day:02d}T{hour:02d}:00:00",
        "exit_time": f"2026-09-{day:02d}T{hour + 1:02d}:00:00",
    }


class TestBootstrapCI:
    def test_degenerate_sample_has_no_interval(self):
        """Одна сделка — интервала нет. None, а не NaN: NaN не равен сам себе,
        и сравнение двух идентичных прогонов падало бы на ровном месте."""
        assert bootstrap_ci([1.0]) is None
        assert bootstrap_ci([]) is None

    def test_is_deterministic_across_runs(self):
        """Интервал не должен «дышать»: иначе золотой тест станет флаки, а
        сравнение двух свипов — невоспроизводимым."""
        values = [2.0, -1.0, 0.25, -1.0, 2.0, 0.25] * 5
        assert bootstrap_ci(values) == bootstrap_ci(values)

    def test_brackets_the_mean(self):
        values = [2.0, -1.0, 0.25] * 20
        lo, hi = bootstrap_ci(values)
        assert lo < sum(values) / len(values) < hi

    def test_noisy_zero_edge_is_not_called_significant(self):
        """Ровно тот случай, ради которого всё затеяно: выборка с нулевым
        мат. ожиданием и большим разбросом не должна проходить как эффект."""
        trades = [x for _ in range(30) for x in (_t(20.0), _t(-20.0))]  # E[R] = 0
        out = summarize(trades)
        assert out["expectancy_R"] == pytest.approx(0.0)
        assert out["expectancy_R_significant"] is False

    def test_clear_edge_is_called_significant(self):
        # Списковое включение, а не [x] * 25: _t обязан вызваться каждый раз,
        # иначе все сделки окажутся в одном дне, то есть в одном блоке.
        trades = [_t(20.0) for _ in range(25)] + [_t(-10.0) for _ in range(5)]
        assert summarize(trades)["expectancy_R_significant"] is True


class TestRNormalisation:
    def test_R_uses_entry_risk_not_position_value(self):
        """R = pnl / бюджет риска на входе."""
        assert summarize([_t(20.0, risk=10.0)])["expectancy_R"] == pytest.approx(2.0)

    def test_varying_risk_is_respected(self):
        """Бюджет риска не константа — его двигают Circuit Breaker и режим
        рынка. Сделка половинного размера должна весить в R столько же."""
        full = summarize([_t(20.0, risk=10.0)])["expectancy_R"]
        half = summarize([_t(10.0, risk=5.0)])["expectancy_R"]
        assert full == half == pytest.approx(2.0)

        mixed = summarize([_t(20.0, risk=10.0), _t(-5.0, risk=5.0)])
        assert mixed["expectancy_R"] == pytest.approx(0.5)  # (2R + -1R) / 2


class TestEquity:
    def test_drawdown_follows_exit_order_not_entry_order(self):
        """Просадку создаёт момент фиксации убытка, а позиции живут до 72 часов
        и закрываются не в том порядке, в котором открывались."""
        early_entry_late_exit = {"pnl": -10.0, "risk": 10.0,
                                 "entry_time": "2026-09-01T00:00:00",
                                 "exit_time": "2026-09-03T00:00:00"}
        late_entry_early_exit = {"pnl": 20.0, "risk": 10.0,
                                 "entry_time": "2026-09-02T00:00:00",
                                 "exit_time": "2026-09-02T01:00:00"}
        m = equity_metrics([early_entry_late_exit, late_entry_early_exit])
        # По времени выхода: сначала +2R, затем -1R → просадка ровно 1R.
        assert m["max_drawdown_R"] == pytest.approx(1.0)

    def test_worst_loss_streak(self):
        trades = [_t(-10.0, hour=h, day=1) for h in range(3)] + [_t(20.0, hour=5, day=1)]
        assert equity_metrics(trades)["worst_loss_streak"] == 3


class TestReporting:
    def test_profit_factor(self):
        assert summarize([_t(20.0), _t(-10.0)])["profit_factor"] == pytest.approx(2.0)

    def test_breakeven_trades_stay_in_the_denominator(self):
        """Безубыточные не исключаются из статистики: иначе winrate становится
        управляемым через partial_close_qty_pct, не меняя при этом денег."""
        trades = [_t(20.0), _t(-10.0), _t(0.5)]
        assert summarize(trades)["trades"] == 3
        assert summarize(trades)["win_rate"] == pytest.approx(66.7, abs=0.1)

    def test_return_to_deposit_is_annualised_per_30d(self):
        out = summarize([_t(20.0)], days=15, deposit=1000.0)
        assert out["return_pct_of_deposit"] == pytest.approx(2.0)
        assert out["return_pct_per_30d"] == pytest.approx(4.0)

    def test_empty_input_keeps_the_same_shape(self):
        """Пустой прогон — обычный исход (фильтр отсёк всё). Форма ответа не
        должна меняться, иначе каждый потребитель проверяет наличие ключей."""
        empty = summarize([])
        assert empty["trades"] == 0
        assert empty["expectancy_R"] is None
        assert empty["expectancy_R_significant"] is False
        assert set(summarize([_t(1.0)])) >= set(empty)


class TestPairedComparison:
    """Парное сравнение двух конфигураций — главный инструмент принятия решений.

    До 22.09.2026 свипы сравнивали конфигурации по отдельным суммам PnL/R без
    оценки неопределённости. Это складывает два независимых шума там, где
    большинство сделок у конфигураций общие и вносят ровно одинаковый вклад.
    """

    @staticmethod
    def _common(n: int = 80) -> list[dict]:
        return [
            {"symbol": f"C{i}", "entry_time": f"2026-09-{i % 25 + 1:02d}T00:00:00",
             "pnl": 20.0 if i % 3 else -10.0, "risk": 10.0}
            for i in range(n)
        ]

    def test_identical_runs_have_zero_width_interval(self):
        """Общие сделки вносят в разницу ровно ноль и не должны раздувать
        интервал — иначе два одинаковых прогона выглядят как расхождение."""
        common = self._common()
        out = compare(common, list(common))
        assert out["delta_R"] == 0.0
        assert out["delta_R_ci"] == (0.0, 0.0)
        assert out["significant"] is False
        assert out["shared"] == len(common) and out["only_a"] == out["only_b"] == 0

    def test_extra_losing_trades_show_up_as_significant_harm(self):
        """Сделка, которую взяла только одна конфигурация, входит в разницу
        целиком: её R против нуля. Это и есть цена решения «брать или нет»."""
        common = self._common()
        # Разнесены по дням: эффект, целиком уместившийся в одни сутки, —
        # одно наблюдение, и значимым он быть не должен.
        extra = [{"symbol": f"X{i}", "entry_time": f"2026-09-{i % 25 + 1:02d}T01:00:00",
                  "pnl": -10.0, "risk": 10.0} for i in range(20)]
        out = compare(common + extra, common)
        assert out["only_a"] == 20
        assert out["delta_R"] == pytest.approx(-20.0)
        assert out["significant"] is True
        assert out["delta_R_ci"][1] < 0

    def test_tiny_difference_is_not_called_significant(self):
        """Одна лишняя сделка на фоне восьмидесяти общих — не эффект."""
        common = self._common()
        extra = [{"symbol": "X", "entry_time": "2026-09-03T01:00:00",
                  "pnl": 20.0, "risk": 10.0}]
        assert compare(common + extra, common)["significant"] is False

    def test_respects_per_trade_risk(self):
        """Сделка половинного размера (Circuit Breaker, cautious-режим) весит
        в R столько же, сколько полноразмерная, — но в долларах вдвое меньше.
        Нормировка на константу этого не видит; ровно эта ошибка была в
        scripts/sweep_circuit_breaker.py, который сам же CB и измерял."""
        full = [{"symbol": "A", "entry_time": "t", "pnl": 20.0, "risk": 10.0}]
        half = [{"symbol": "A", "entry_time": "t", "pnl": 10.0, "risk": 5.0}]
        out = compare(full, half)
        assert out["delta_R"] == pytest.approx(0.0), "обе сделки — ровно +2R"

    def test_empty_inputs(self):
        assert compare([], [])["significant"] is False


class TestEmptyRunIsPrintable:
    """Конфигурация без единой сделки — законный исход свипа (порог отсёк всё).
    Раньше на ней падали три скрипта: None не форматируется."""

    def test_fmt_shows_dash_not_zero(self):
        """Прочерк, а не ноль: ноль читается как измеренное значение."""
        assert fmt(None) == "—"
        assert fmt(1.5) == "+1.50"
        assert fmt(1.5, ".1f") == "1.5"

    def test_empty_summary_has_days_keys_when_days_given(self):
        out = summarize([], days=10, deposit=1000.0)
        assert out["R_per_day"] == 0.0
        assert out["return_pct_per_30d"] == 0.0
        assert out["expectancy_R"] is None, "мат. ожидание не определено"
        assert out["total_R"] == 0.0, "сумма определена и равна нулю"


class TestBlockBootstrap:
    """Блочный bootstrap: сделки не независимы, одно движение рынка даёт
    несколько сигналов разом.

    Замер 22.09.2026 на боевых данных: из разницы бэктеста с реалом в +15.26R
    54% пришлось на ОДИН час (девять сделок 16.09 18:00). Обычный bootstrap по
    сделкам дал [+0.48; +30.10] — «значимо». Блочный по суткам дал
    [-1.49; +36.32] — «не значимо». Первый ответ был ложноположительным.
    """

    def test_effect_confined_to_one_day_is_not_significant(self):
        """Двадцать прекрасных сделок в один день — это одно наблюдение."""
        base = [_t(-1.0, day=d) for d in range(1, 26)]
        spike = [_t(50.0, day=7) for _ in range(20)]
        out = summarize(base + spike)
        assert out["expectancy_R"] > 0, "точечная оценка положительна"
        assert out["expectancy_R_significant"] is False, "но держится на одном дне"

    def test_same_effect_spread_over_days_is_significant(self):
        """Тот же суммарный эффект, разнесённый по дням, значим."""
        base = [_t(-1.0, day=d) for d in range(1, 26)]
        spread = [_t(50.0, day=d % 25 + 1) for d in range(20)]
        assert summarize(base + spread)["expectancy_R_significant"] is True

    def test_single_block_has_no_interval(self):
        """Все сделки в одном дне — блоков меньше двух, интервала нет."""
        assert bootstrap_ci([1.0, 2.0, 3.0], blocks=["d1", "d1", "d1"]) is None


# ---------------------------------------------------------------------------
# Безубыток — отдельный класс исхода
# ---------------------------------------------------------------------------
#
# До 23.09.2026 сделка, снятую с безубыточного стопа после частичной фиксации,
# отчёты считали успешной: PnL у неё положительный (забронированная на триггере
# часть минус комиссии), а классификация шла по знаку PnL. Для winrate это
# означало, что рынок вернулся к цене входа, сетап не отработал, а статистика
# рапортует плюс.


def _trade(entry, exit_price, pnl, **over):
    from src.backtest.metrics import BREAKEVEN_BAND_PCT  # noqa: F401
    return {"entry_price": entry, "exit_price": exit_price, "pnl": pnl, **over}


def test_breakeven_exit_is_not_a_win():
    from src.backtest.metrics import BREAKEVEN, outcome

    # Частичная фиксация 20% на +3.5%, остаток снят стопом ровно на входе:
    # PnL положительный, но исход — безубыток.
    assert outcome(_trade(100.0, 100.0, +0.14)) == BREAKEVEN


def test_full_take_and_full_stop_keep_their_classes():
    from src.backtest.metrics import LOSS, WIN, outcome

    assert outcome(_trade(100.0, 110.0, +2.0)) == WIN
    assert outcome(_trade(100.0, 95.0, -1.0)) == LOSS


def test_band_edges():
    from src.backtest.metrics import BREAKEVEN, LOSS, WIN, outcome

    assert outcome(_trade(100.0, 100.5, +0.2)) == BREAKEVEN      # ровно на границе
    assert outcome(_trade(100.0, 100.6, +0.2)) == WIN
    assert outcome(_trade(100.0, 99.5, -0.1)) == BREAKEVEN
    assert outcome(_trade(100.0, 99.4, -0.1)) == LOSS


def test_short_direction_is_mirrored():
    from src.backtest.metrics import LOSS, WIN, outcome

    assert outcome(_trade(100.0, 95.0, +1.0, direction="short")) == WIN
    assert outcome(_trade(100.0, 105.0, -1.0, direction="short")) == LOSS


def test_classification_does_not_depend_on_partial_share():
    """Ключевое свойство: доля партиала меняет РАЗМЕР результата, но не класс.

    Классификация по механизму («был партиал и вышли по стопу») сделала бы
    winrate управляемым через partial_close_qty_pct без изменения денег. По цене
    выхода этого не происходит.
    """
    from src.backtest.metrics import outcome

    same_exit = [_trade(100.0, 100.0, pnl) for pnl in (0.05, 0.14, 0.35, 0.70)]
    assert {outcome(t) for t in same_exit} == {"breakeven"}


def test_missing_prices_fall_back_to_pnl_sign():
    """Старые прогоны и частичные выгрузки не должны ломать форму отчёта."""
    from src.backtest.metrics import LOSS, WIN, outcome

    assert outcome({"pnl": +1.0}) == WIN
    assert outcome({"pnl": -1.0}) == LOSS


def test_counts_keep_breakeven_in_the_denominator():
    from src.backtest.metrics import outcome_counts

    c = outcome_counts([
        _trade(100.0, 110.0, +2.0),
        _trade(100.0, 100.0, +0.14),
        _trade(100.0, 100.0, +0.14),
        _trade(100.0, 95.0, -1.0),
    ])
    assert (c["wins"], c["breakevens"], c["losses"]) == (1, 2, 1)
    # 1 из 4, а не 1 из 2: вынести безубытки из знаменателя — снова сделать
    # winrate управляемым долей партиала.
    assert c["win_rate"] == 25.0
    assert c["breakeven_rate"] == 50.0
    assert c["loss_rate"] == 25.0


def test_summarize_reports_all_three_classes():
    from src.backtest.metrics import summarize

    out = summarize([
        dict(_trade(100.0, 110.0, +2.0), risk=1.0, entry_time="2026-09-01T00:00:00"),
        dict(_trade(100.0, 100.0, +0.14), risk=1.0, entry_time="2026-09-02T00:00:00"),
        dict(_trade(100.0, 95.0, -1.0), risk=1.0, entry_time="2026-09-03T00:00:00"),
    ])
    assert (out["wins"], out["breakevens"], out["losses"]) == (1, 1, 1)
    assert out["win_rate"] == 33.3


def test_empty_summary_has_the_new_keys():
    from src.backtest.metrics import summarize

    out = summarize([])
    for key in ("wins", "breakevens", "losses", "win_rate", "breakeven_rate", "loss_rate"):
        assert key in out


# ---------------------------------------------------------------------------
# Безубыток засчитывается в победы пропорционально прибыли от полного тейка
# ---------------------------------------------------------------------------


def test_breakeven_credit_matches_the_arithmetic():
    from src.backtest.metrics import breakeven_credit

    # доля 20%, RR 2.0, триггер 35% пути до TP:
    # забронировано 0.20*2.0*0.35 = 0.14R, полный тейк 0.14 + 0.80*2.0 = 1.74R
    assert breakeven_credit(2.0, 35.0, 20.0) == pytest.approx(0.14 / 1.74, rel=1e-9)
    assert 1 / breakeven_credit(2.0, 35.0, 20.0) == pytest.approx(12.43, abs=0.01)


def test_no_partial_means_no_credit():
    """Доля 0% — на триггере не бронируется ничего, зачитывать нечего."""
    from src.backtest.metrics import breakeven_credit

    assert breakeven_credit(2.0, 35.0, 0.0) == 0.0


def test_weighted_win_rate_credits_breakevens():
    from src.backtest.metrics import breakeven_credit, outcome_counts

    trades = ([_trade(100.0, 110.0, +2.0)] * 27
              + [_trade(100.0, 100.0, +0.12)] * 41
              + [_trade(100.0, 95.0, -1.0)] * 40)
    c = outcome_counts(trades, be_credit=breakeven_credit(2.0, 35.0, 20.0))
    assert (c["wins"], c["breakevens"], c["losses"]) == (27, 41, 40)
    assert c["win_rate"] == 25.0                       # три колонки не меняются
    assert c["breakeven_equivalent_wins"] == pytest.approx(3.3, abs=0.05)
    assert c["win_rate_weighted"] == pytest.approx(28.1, abs=0.1)


def test_unprofitable_breakeven_gets_no_credit():
    """Комиссия съела забронированную часть — зачитывать нечего."""
    from src.backtest.metrics import outcome_counts

    c = outcome_counts([_trade(100.0, 100.0, -0.01)] * 10, be_credit=0.08)
    assert c["breakevens"] == 10
    assert c["breakeven_equivalent_wins"] == 0.0
    assert c["win_rate_weighted"] == 0.0


def test_weighted_keys_absent_without_credit():
    """Без веса отчёт остаётся прежним — колонка просто не появляется."""
    from src.backtest.metrics import outcome_counts

    c = outcome_counts([_trade(100.0, 100.0, +0.12)])
    assert "win_rate_weighted" not in c
    assert "breakeven_credit" not in c


def test_weighted_win_rate_is_not_a_decision_metric():
    """Вес растёт с долей партиала, а ΣR при этом падает — поэтому взвешенный
    winrate отчётный, а решение принимается по E[R] (AGENTS.md, правило 8).

    Свип 22.09.2026 на двух БД: ΣR убывает монотонно с ростом доли (−0.036R на
    каждые +10 п.п. на 27.08-22.09). Вес же растёт, потому что растёт и
    забронированная часть, и одновременно обесценивается полный тейк.
    """
    from src.backtest.metrics import breakeven_credit

    assert (breakeven_credit(2.0, 35.0, 20.0)
            < breakeven_credit(2.0, 35.0, 50.0)
            < breakeven_credit(2.0, 35.0, 80.0))
