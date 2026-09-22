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


def _t(pnl: float, risk: float = 10.0, hour: int = 0) -> dict:
    return {
        "pnl": pnl,
        "risk": risk,
        "entry_time": f"2026-09-01T{hour:02d}:00:00",
        "exit_time": f"2026-09-01T{hour + 1:02d}:00:00",
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
        trades = [_t(20.0), _t(-20.0)] * 30   # ровно нулевое мат. ожидание
        out = summarize(trades)
        assert out["expectancy_R"] == pytest.approx(0.0)
        assert out["expectancy_R_significant"] is False

    def test_clear_edge_is_called_significant(self):
        trades = [_t(20.0)] * 25 + [_t(-10.0)] * 5
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
        trades = [_t(-10.0, hour=h) for h in range(3)] + [_t(20.0, hour=5)]
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
            {"symbol": f"C{i}", "entry_time": f"2026-09-0{i % 9 + 1}T00:00:00",
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
        extra = [{"symbol": f"X{i}", "entry_time": "2026-09-01T00:00:00",
                  "pnl": -10.0, "risk": 10.0} for i in range(20)]
        out = compare(common + extra, common)
        assert out["only_a"] == 20
        assert out["delta_R"] == pytest.approx(-20.0)
        assert out["significant"] is True
        assert out["delta_R_ci"][1] < 0

    def test_tiny_difference_is_not_called_significant(self):
        """Одна лишняя сделка на фоне восьмидесяти общих — не эффект."""
        common = self._common()
        extra = [{"symbol": "X", "entry_time": "2026-09-01T00:00:00",
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
