"""
Золотой тест движка бэктеста (`src/backtest/engine.py`).

Движок — инструмент, которым принято КАЖДОЕ решение по параметрам стратегии, и
до 26.08.2026 у него не было ни одного теста при задокументированной истории
parity-багов: реализаций цикла было три (runner.py, sweep_retracement.py,
sweep_rr_sl.py), они совпадали на 62% строк, и когда в скриптах отсутствовал
`oi_declining`, он завысил результаты свипов по RR, partial-close и retracement —
выяснилось это сильно позже принятых на них решений.

Тест строит маленькую синтетическую БД с заранее известным исходом и фиксирует
результат целиком. Любое изменение поведения движка — намеренное или случайно
приехавшее из детектора — ломает его и требует осознанного обновления эталона.
"""

import sqlite3
from datetime import datetime, timedelta

import pytest

from src.analytics.detector import SetupDetector
from src.backtest.engine import (
    BACKTEST_VIRTUAL_BALANCE,
    _round_to_lot,
    build_volume_gate_mask,
    load_data,
    simulate,
)
from src.config import CollectorsConfig, Settings, StrategyConfig, TradingConfig

BASE_TS = datetime(2026, 8, 1, 0, 0, 0)
SYMBOL = "GOLD/USDT:USDT"
BAR_MINUTES = 3


# ---------------------------------------------------------------------------
# Фикстура: свечи с рукотворным всплеском объёма и последующим ростом цены
# ---------------------------------------------------------------------------


def _write_db(path, candles, oi_points, mc_rows=()):
    db = sqlite3.connect(str(path))
    db.executescript(
        """
        CREATE TABLE candles (
            id INTEGER PRIMARY KEY, exchange TEXT, symbol TEXT, timestamp TEXT,
            open REAL, high REAL, low REAL, close REAL, volume REAL);
        CREATE TABLE open_interest (
            id INTEGER PRIMARY KEY, exchange TEXT, symbol TEXT, timestamp TEXT, value REAL);
        CREATE TABLE market_context_snapshots (
            id INTEGER PRIMARY KEY, timestamp TEXT, regime TEXT, supertrend_color TEXT);
        """
    )
    db.executemany(
        "INSERT INTO candles (exchange,symbol,timestamp,open,high,low,close,volume)"
        " VALUES (?,?,?,?,?,?,?,?)",
        candles,
    )
    db.executemany(
        "INSERT INTO open_interest (exchange,symbol,timestamp,value) VALUES (?,?,?,?)",
        oi_points,
    )
    db.executemany(
        "INSERT INTO market_context_snapshots (timestamp,regime,supertrend_color)"
        " VALUES (?,?,?)",
        mc_rows,
    )
    db.commit()
    db.close()


def _ts(i: int) -> str:
    return (BASE_TS + timedelta(minutes=BAR_MINUTES * i)).strftime("%Y-%m-%d %H:%M:%S")


def _build_candles(n_baseline: int, n_sustain: int, n_after: int):
    """Спокойный baseline → всплеск объёма с ростом цены → плавный рост до TP.

    Форма подобрана так, чтобы пройти фильтры детектора на дефолтном конфиге:
    рост внутри sustain-окна умеренный (не exhaustion), объём монотонно растёт
    (не volume_fading/declining), до окна цена стоит (не pre_surge_pump).
    """
    rows = []
    price = 100.0
    # baseline: цена стоит, объём ровный
    for i in range(n_baseline):
        rows.append(("bybit", SYMBOL, _ts(i), price, price * 1.001, price * 0.999, price, 1_000.0))
    # sustain: объём кратно выше нормы, цена растёт по чуть-чуть
    for j in range(n_sustain):
        i = n_baseline + j
        nxt = price * 1.012
        rows.append(("bybit", SYMBOL, _ts(i), price, nxt * 1.001, price * 0.999, nxt, 8_000.0 + j * 500))
        price = nxt
    # after: спокойный рост дальше — позиция дойдёт до TP
    for j in range(n_after):
        i = n_baseline + n_sustain + j
        nxt = price * 1.004
        rows.append(("bybit", SYMBOL, _ts(i), price, nxt * 1.002, price * 0.998, nxt, 1_200.0))
        price = nxt
    return rows


def _build_oi(n_total: int):
    """OI монотонно растёт — проходит и oi_declining, и oi_slope_min_pct."""
    return [
        ("bybit", SYMBOL, _ts(i), 1_000_000.0 * (1 + 0.01 * i))
        for i in range(n_total)
    ]


def _settings(**strategy_overrides) -> Settings:
    params = dict(
        exclude_coins=[],
        baseline_bars=20,
        volume_surge_mult=5.0,
        sustain_bars=4,
        oi_filter_enabled=True,
        oi_slope_min_pct=0.0,
        price_growth_min_pct=1.0,
        price_growth_max_pct=25.0,
        min_volume_usdt=0.0,
        min_baseline_volume_usdt=0.0,
    )
    params.update(strategy_overrides)
    strategy = StrategyConfig(**params)
    trading = TradingConfig(
        mode="real",
        max_positions=5,
        leverage=10,
        risk_per_trade_pct=1.0,
        risk_reward_ratio=2.0,
        stop_loss_pct=5.0,
        max_hold_hours=48.0,
        partial_close_pct=35.0,
        partial_close_qty_pct=30.0,
        cooldown_hours=1.0,
        pending_entry_pullback_pct=0.0,
        backtest_slippage_pct=0.0,
        circuit_breaker_enabled=True,
    )
    return Settings(
        exchanges={},
        collectors=CollectorsConfig(timeframe=f"{BAR_MINUTES}m"),
        strategy=strategy,
        trading=trading,
    )


@pytest.fixture
def golden_db(tmp_path):
    n_baseline, n_sustain, n_after = 20, 4, 60
    path = tmp_path / "golden.db"
    _write_db(
        path,
        _build_candles(n_baseline, n_sustain, n_after),
        _build_oi(n_baseline + n_sustain + n_after),
    )
    return str(path)


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


def test_load_data_shapes(golden_db):
    """Загрузчик отдаёт индексы, на которые опирается O(1)-поиск свечи."""
    data = load_data(golden_db)
    assert set(data) == {
        "symbols", "sym_ts_to_row", "sym_ts_to_idx",
        "all_timestamps", "oi_cache", "mc_snapshots", "mc_ts_list",
    }
    assert list(data["symbols"]) == [SYMBOL]
    assert len(data["all_timestamps"]) == 84
    assert data["sym_ts_to_idx"][SYMBOL][data["all_timestamps"][0]] == 0
    assert ("bybit", SYMBOL) in data["oi_cache"]


def test_loader_drops_zero_volume_bars(tmp_path):
    """Бары с нулевым объёмом не должны попадать в окно детектора.

    Боевой `DataProvider.load_candles` их отбрасывает (незакрытые/пустые), и окно
    у него схлопывается на таком баре. Пока фильтра не было в движке, окна
    расходились с проливом уже на входных данных — на архивной БД за 10.08-25.08
    это 60 395 лишних баров.
    """
    path = tmp_path / "zero_vol.db"
    candles = _build_candles(20, 4, 10)
    # каждый третий бар — «пустой»
    candles = [
        (c[0], c[1], c[2], c[3], c[4], c[5], c[6], 0.0) if i % 3 == 0 else c
        for i, c in enumerate(candles)
    ]
    _write_db(path, candles, _build_oi(34))

    data = load_data(str(path))
    volumes = [bar[5] for bar in data["symbols"][SYMBOL]]
    assert volumes, "что-то должно остаться"
    assert all(v > 0 for v in volumes), "нулевые бары обязаны быть отфильтрованы"


def test_loader_does_not_merge_exchanges(tmp_path):
    """Монета на двух биржах — берётся ОДИН ряд, а не склейка двух.

    Коллектор пишет свечи только с одной биржи на монету, но на старых БД пары
    встречаются. При группировке по одному символу ряды склеивались в один с
    дублирующимися timestamp'ами, и окно детектора собиралось из перемешанных бирж.
    """
    path = tmp_path / "two_exchanges.db"
    bybit = _build_candles(20, 4, 5)                     # 29 баров
    binance = [("binance",) + c[1:] for c in _build_candles(20, 4, 40)]  # 64 бара
    _write_db(path, bybit + binance, _build_oi(64))

    data = load_data(str(path))
    bars = data["symbols"][SYMBOL]
    timestamps = [b[0] for b in bars]
    assert len(timestamps) == len(set(timestamps)), "дублирующихся timestamp быть не должно"
    assert len(bars) == 64, "должен остаться ряд с большей историей (binance)"


def test_candle_slice_matches_live_detector_window(golden_db, monkeypatch):
    """Окно, которое движок отдаёт детектору, должно быть той же длины, что в проливе.

    Боевой `SetupDetector.analyze()` грузит `baseline_bars + sustain_bars + 10` баров.
    Без `+ 1` в диапазоне срез получался на один бар длиннее, и baseline (первые
    `baseline_bars` элементов) съезжал на бар назад — сдвигалась медиана объёма, а
    с ней и порог всплеска. Фикс был в runner.py и терялся при унификации движков.
    """
    settings = _settings()
    expected = (
        settings.strategy.baseline_bars + settings.strategy.sustain_bars + 10
    )

    seen: list[int] = []
    import src.analytics.detector as det_mod
    original = det_mod.SetupDetector.check_volume_pattern

    def spy(self, candles, context=None):
        seen.append(len(candles))
        return original(self, candles, context)

    monkeypatch.setattr(det_mod.SetupDetector, "check_volume_pattern", spy)
    # prefilter=False намеренно: предмет теста — геометрия среза, а предфильтр
    # решает лишь, строить ли срез вообще, и на баре без кандидата детектор
    # просто не вызывается. Что предфильтр не съедает настоящих кандидатов,
    # проверяет TestVolumeGatePrefilter.
    simulate(settings, load_data(golden_db), has_oi=True, prefilter=False)

    assert seen, "детектор должен был вызываться"
    # Ближе к началу истории баров меньше — важен максимум (полное окно)
    assert max(seen) == expected, (
        f"полное окно должно быть {expected} баров, а не {max(seen)}"
    )


def test_golden_run_is_stable(golden_db):
    """Эталон: на этой фикстуре движок обязан дать ровно такой результат.

    Если тест упал — движок изменил поведение. Это либо намеренная правка (тогда
    обнови ожидания осознанно и опиши в коммите, что и почему сдвинулось), либо
    незамеченная регрессия, ради которой тест и написан.
    """
    result = simulate(_settings(), load_data(golden_db), has_oi=True)

    assert result["signals"] == 1, "всплеск объёма ровно один"
    assert result["trades"] == 1
    assert result["wins"] == 1 and result["losses"] == 0
    assert result["win_rate"] == 100.0
    assert result["tp_wins"] == 1
    assert result["sl_losses"] == 0
    assert result["time_exits"] == 0
    assert result["partials"] == 1, "цена проходит порог частичной фиксации по пути к TP"

    trade = result["trades_list"][0]
    assert trade["symbol"] == SYMBOL
    assert trade["exit_reason"] == "tp"
    assert trade["partial_closed"] is True
    # TP = вход + (вход × SL%) × RR = +10% при SL=5%, RR=2.0
    assert trade["tp_price"] == pytest.approx(trade["entry_price"] * 1.10, rel=1e-9)
    # После частичной фиксации стоп переводится в безубыток — исходный -5% уже не действует
    assert trade["sl_price"] == pytest.approx(trade["entry_price"], rel=1e-9)
    assert trade["pnl"] > 0
    assert result["total_fees"] > 0, "комиссии обязаны учитываться (parity с реалом)"


def test_oi_declining_blocks_signal(golden_db, tmp_path):
    """Падающий OI режет сигнал.

    Именно эта проверка отсутствовала во всех свип-скриптах и завысила прошлые
    свипы — она обязана жить в движке, а не в копии цикла.
    """
    n_baseline, n_sustain, n_after = 20, 4, 60
    total = n_baseline + n_sustain + n_after
    path = tmp_path / "declining_oi.db"
    # OI плавно снижается на каждом баре. Порог наклона в этом тесте опущен ниже
    # фактического, поэтому единственное, что может зарезать сигнал, — сама
    # проверка oi_declining, а не oi_slope_min_pct.
    oi = [
        ("bybit", SYMBOL, _ts(i), 1_000_000.0 * (1 - 0.001 * i))
        for i in range(total)
    ]
    _write_db(path, _build_candles(n_baseline, n_sustain, n_after), oi)

    baseline = simulate(_settings(), load_data(golden_db), has_oi=True)
    assert baseline["signals"] == 1  # контроль: на исходной фикстуре сигнал есть

    lenient_slope = {"oi_slope_min_pct": -50.0}
    declining = simulate(
        _settings(**lenient_slope), load_data(str(path)), has_oi=True
    )
    assert declining["signals"] == 0

    # Флаг выключает проверку — иначе порог было бы нечем свипнуть
    off = simulate(
        _settings(oi_declining_enabled=False, **lenient_slope),
        load_data(str(path)), has_oi=True,
    )
    assert off["signals"] == 1


def test_oi_trend_window_changes_verdict(tmp_path, golden_db):
    """Временное окно OI подключено в движке и меняет вердикт на тех же данных.

    Форма данных: OI стоит на месте и разгоняется на двух барах, последний из
    которых — бар сигнала. Прежний режим берёт три последние СТРОКИ, то есть
    два каданса скана, и к моменту решения видит уже выполаживание
    (1.02 -> 1.04 -> 1.04, наклон 2.9%). Двенадцатиминутное окно видит весь
    разгон от плоскости (наклон 4.7%). Один и тот же порог 3% даёт
    противоположные ответы — не потому, что окно строже или мягче, а потому,
    что это разные величины: скорость за два каданса против прироста за
    sustain-окно детектора.
    """
    n_baseline, n_sustain, n_after = 20, 4, 60
    total = n_baseline + n_sustain + n_after
    # Разгон OI заканчивается на последнем баре sustain-окна, а решение движок
    # принимает на следующем — поэтому к моменту решения ряд уже плоский.
    ramp_top = n_baseline + n_sustain - 1

    def oi_at(i: int) -> float:
        if i >= ramp_top:
            return 1_040_000.0
        if i == ramp_top - 1:
            return 1_020_000.0
        return 1_000_000.0

    path = tmp_path / "oi_window.db"
    oi = [("bybit", SYMBOL, _ts(i), oi_at(i)) for i in range(total)]
    _write_db(path, _build_candles(n_baseline, n_sustain, n_after), oi)
    data = load_data(str(path))

    legacy = simulate(_settings(oi_slope_min_pct=3.0), data, has_oi=True)
    windowed = simulate(
        _settings(oi_slope_min_pct=3.0, oi_trend_window_bars=4), data, has_oi=True,
    )
    assert legacy["signals"] == 0, "три последние точки застали уже выполаживание"
    assert windowed["signals"] == 1, "окно застало весь разгон"

    # Контроль с обеих сторон: окно не пропускает что угодно и не режет что угодно.
    for th, expected in ((2.0, 1), (5.0, 0)):
        both = [
            simulate(_settings(oi_slope_min_pct=th, oi_trend_window_bars=w), data,
                     has_oi=True)["signals"]
            for w in (0, 4)
        ]
        assert both == [expected, expected], f"порог {th}: {both}"


def test_oi_trend_window_default_is_legacy(tmp_path, golden_db):
    """Дефолт параметра (0) обязан повторять прежнее поведение до сделки."""
    data = load_data(golden_db)
    legacy = simulate(_settings(), data, has_oi=True)
    explicit = simulate(_settings(oi_trend_window_bars=0), data, has_oi=True)
    assert legacy["signals"] == explicit["signals"]
    assert [t["entry_time"] for t in legacy["trades_list"]] == [
        t["entry_time"] for t in explicit["trades_list"]
    ]


def test_oi_filter_disabled_lets_signals_through(tmp_path, golden_db):
    """`oi_filter_enabled=false` обязан снимать ГЕЙТ ЦЕЛИКОМ.

    Регрессия 26.08.2026, найдена пользователем. При унификации движков проверка
    флага потерялась (`if has_oi:` вместо `if has_oi and cfg.oi_filter_enabled:`),
    и гейт, выключенный в проде 25.08.2026, молча вернулся в строй: на архивной БД
    за 10.08-25.08 выборка упала с 49 сигналов до 8. Флаг выключен в боевом
    config.yaml, так что именно этот путь и работает в реальности.
    """
    n_baseline, n_sustain, n_after = 20, 4, 60
    total = n_baseline + n_sustain + n_after
    path = tmp_path / "bad_oi.db"
    # OI и падает на последней точке, и имеет отрицательный наклон — гейт
    # зарубил бы сигнал по обеим причинам сразу
    oi = [
        ("bybit", SYMBOL, _ts(i), 1_000_000.0 * (1 - 0.01 * i))
        for i in range(total)
    ]
    _write_db(path, _build_candles(n_baseline, n_sustain, n_after), oi)

    with_gate = simulate(_settings(oi_filter_enabled=True), load_data(str(path)), has_oi=True)
    assert with_gate["signals"] == 0, "при включённом гейте плохой OI режет сигнал"

    without_gate = simulate(_settings(oi_filter_enabled=False), load_data(str(path)), has_oi=True)
    assert without_gate["signals"] == 1, "при выключенном гейте OI не должен влиять вообще"
    assert without_gate["trades"] == 1


def test_risk_off_blocks_entries(golden_db, tmp_path):
    """risk_off из market_context_snapshots запрещает открытие позиций."""
    n_baseline, n_sustain, n_after = 20, 4, 60
    total = n_baseline + n_sustain + n_after
    path = tmp_path / "risk_off.db"
    _write_db(
        path,
        _build_candles(n_baseline, n_sustain, n_after),
        _build_oi(total),
        mc_rows=[(_ts(0), "risk_off", "red")],
    )
    result = simulate(_settings(), load_data(str(path)), has_oi=True)
    assert result["signals"] == 0, "в risk_off детекция до входа не доходит"
    assert result["trades"] == 0


def test_risk_off_still_manages_open_positions(tmp_path):
    """risk_off запрещает НОВЫЕ входы, но не замораживает уже открытую позицию.

    Регрессия 26.08.2026: `continue` по режиму стоял до блока сопровождения, и
    в risk_off-окне открытые позиции переставали проверяться на TP/SL и на
    max_hold_hours. В проде так не бывает — `update_positions()` идёт каждый цикл.
    """
    n_baseline, n_sustain, n_after = 20, 4, 60
    total = n_baseline + n_sustain + n_after
    path = tmp_path / "risk_off_midway.db"
    # Режим спокойный на входе и уходит в risk_off сразу после открытия позиции
    _write_db(
        path,
        _build_candles(n_baseline, n_sustain, n_after),
        _build_oi(total),
        mc_rows=[
            (_ts(0), "risk_on", "green"),
            (_ts(n_baseline + n_sustain + 1), "risk_off", "red"),
        ],
    )
    result = simulate(_settings(), load_data(str(path)), has_oi=True)

    assert result["trades"] == 1, "позиция успела открыться до risk_off"
    assert result["trades_list"][0]["exit_reason"] == "tp", (
        "и должна быть закрыта по TP внутри risk_off-окна, а не зависнуть"
    )


def test_exclude_coins_skips_symbol(golden_db):
    """Монета из exclude_coins не даёт сигналов вовсе."""
    result = simulate(_settings(exclude_coins=["GOLD"]), load_data(golden_db), has_oi=True)
    assert result["signals"] == 0
    assert result["trades"] == 0


def test_runner_delegates_to_engine(golden_db, tmp_path, monkeypatch):
    """`runner.run_backtest` обязан быть обёрткой, а не второй реализацией.

    Раньше это были два независимых цикла, разошедшихся в существенном.
    """
    import src.backtest.runner as runner

    settings = _settings()
    monkeypatch.setattr(runner.Settings, "from_yaml", staticmethod(lambda _p: settings))

    via_runner = runner.run_backtest("ignored.yaml", golden_db, has_oi=True)
    via_engine = simulate(settings, load_data(golden_db), has_oi=True)

    for key in ("signals", "trades", "wins", "losses", "total_pnl", "tp_wins", "partials"):
        assert via_runner[key] == via_engine[key], f"расхождение по {key}"
    assert via_runner["trades_list"] == via_engine["trades_list"]


# ---------------------------------------------------------------------------
# Circuit Breaker: parity со знаком PnL, а не с причиной выхода
# ---------------------------------------------------------------------------


def _build_partial_then_be(symbol: str, n_baseline: int, n_sustain: int, start_bar: int = 0):
    """Всплеск объёма → цена доходит до партиал-триггера → возврат ко входу.

    Выход получается по `sl` (б/у-стоп), но PnL положительный: доля позиции уже
    забронирована на 35% пути к TP. Боевой `PositionManager._close_position`
    считает такую сделку ПРИБЫЛЬНОЙ и сбрасывает серию убытков.
    """
    rows = []
    price = 100.0
    for i in range(n_baseline):
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            price, price * 1.001, price * 0.999, price, 1_000.0,
        ))
    for j in range(n_sustain):
        i = n_baseline + j
        nxt = price * 1.012
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            price, nxt * 1.001, price * 0.999, nxt, 8_000.0 + j * 500,
        ))
        price = nxt
    entry = price
    # рост выше партиал-триггера (+3.5% при SL=5%, RR=2.0, partial_close_pct=35)
    for j, mult in enumerate((1.02, 1.05, 1.05)):
        i = n_baseline + n_sustain + j
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            entry, entry * mult, entry * 0.999, entry * mult * 0.99, 1_200.0,
        ))
    # возврат ровно ко входу — срабатывает б/у-стоп
    for j in range(3, 40):
        i = n_baseline + n_sustain + j
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            entry, entry * 1.001, entry * 0.98, entry * 0.99, 1_200.0,
        ))
    return rows


def _build_straight_to_sl(symbol: str, n_baseline: int, n_sustain: int, start_bar: int):
    """Всплеск объёма → цена сразу валится ниже стопа. Полный SL без партиала."""
    rows = []
    price = 100.0
    for i in range(n_baseline):
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            price, price * 1.001, price * 0.999, price, 1_000.0,
        ))
    for j in range(n_sustain):
        i = n_baseline + j
        nxt = price * 1.012
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            price, nxt * 1.001, price * 0.999, nxt, 8_000.0 + j * 500,
        ))
        price = nxt
    entry = price
    for j in range(40):
        i = n_baseline + n_sustain + j
        rows.append((
            "bybit", symbol, _ts(start_bar + i),
            entry, entry * 1.0005, entry * 0.90, entry * 0.91, 1_200.0,
        ))
    return rows


@pytest.fixture
def cb_parity_db(tmp_path):
    """Две монеты подряд: первая выходит по б/у-стопу в плюс, вторая — в полный SL."""
    n_baseline, n_sustain = 20, 4
    first = _build_partial_then_be("AAA/USDT:USDT", n_baseline, n_sustain, start_bar=0)
    second = _build_straight_to_sl("BBB/USDT:USDT", n_baseline, n_sustain, start_bar=44)
    candles = first + second
    oi = [
        ("bybit", sym, _ts(i), 1_000_000.0 * (1 + 0.01 * i))
        for sym in ("AAA/USDT:USDT", "BBB/USDT:USDT")
        for i in range(120)
    ]
    path = tmp_path / "cb.db"
    _write_db(path, candles, oi)
    return str(path)


def test_be_stop_exit_does_not_feed_circuit_breaker(cb_parity_db):
    """Прибыльный выход по стопу обязан СБРАСЫВАТЬ серию убытков, как в проливе.

    Движок инкрементировал `cb_losses` на любом `exit_reason == "sl"`, не глядя на
    знак PnL. Боевой `PositionManager._close_position` смотрит именно на знак
    (`if (trade.pnl or 0) <= 0`), поэтому б/у-выход после партиала в проливе —
    победа. Расхождение завышало срабатывания Circuit Breaker в бэктесте и
    искажало любой свип, где менялась доля партиала.

    Мутация для проверки теста: вернуть в движке безусловный `cb_losses += 1` —
    вторая сделка откроется половинным размером и |PnL| упадёт вдвое.
    """
    settings = _settings()
    settings.trading.circuit_breaker_loss_streak_reduce = 1
    settings.trading.circuit_breaker_loss_streak_stop = 99  # полную остановку не проверяем
    settings.trading.cooldown_hours = 0.0

    result = simulate(settings, load_data(cb_parity_db), has_oi=True)
    trades = {t["symbol"]: t for t in result["trades_list"]}

    assert "AAA/USDT:USDT" in trades and "BBB/USDT:USDT" in trades, result["trades_list"]
    first = trades["AAA/USDT:USDT"]
    assert first["exit_reason"] == "sl", "выход по б/у-стопу проходит по ветке sl"
    assert first["partial_closed"] is True
    assert first["pnl"] > 0, "доля забронирована на 35% пути — сделка прибыльная"

    second = trades["BBB/USDT:USDT"]
    assert second["exit_reason"] == "sl"
    # Риск на сделку = virtual_balance 1000 × risk_per_trade_pct 1% = $10.
    # Полный размер даёт убыток около -$10, уменьшенный вдвое — около -$5.
    assert second["pnl"] < -7.0, (
        f"вторая сделка открыта уменьшенным размером (PnL={second['pnl']:.2f}) — "
        "значит прибыльный б/у-выход ошибочно засчитан в серию убытков"
    )


# ---------------------------------------------------------------------------
# Метрики решения: знаменатель R
# ---------------------------------------------------------------------------
#
# Регресс-ловушка, стоившая неверного вывода при аудите 22.09.2026: и прод, и
# движок уменьшают quantity при частичном закрытии. Поэтому R, посчитанный как
# pnl/(entry_price*quantity), завышается в 1/(1-partial_close_qty_pct) раз у
# КАЖДОЙ сделки, дошедшей до партиала, — а таких около 60%. Единственный верный
# знаменатель — бюджет риска на момент входа, и движок обязан его сохранять.


def test_trades_carry_entry_risk_budget(golden_db):
    """Каждая закрытая сделка несёт risk — бюджет риска на ВХОДЕ."""
    settings = _settings()
    result = simulate(settings, load_data(golden_db), has_oi=True)

    assert result["trades"] >= 1
    expected = BACKTEST_VIRTUAL_BALANCE * settings.trading.risk_per_trade_pct / 100
    for t in result["trades_list"]:
        assert t["risk"] == pytest.approx(expected), t


def test_R_is_not_inflated_by_partial_close(golden_db):
    """R считается от исходного риска, а не от остатка объёма.

    В золотом прогоне сделка доходит до партиала и до TP. Наивный знаменатель
    `entry_price * quantity` описывал бы уже урезанную позицию и завышал R —
    проверяем, что движок отдаёт честный.
    """
    settings = _settings()
    result = simulate(settings, load_data(golden_db), has_oi=True)
    trade = result["trades_list"][0]
    assert trade["partial_closed"] is True, "фикстура должна доходить до партиала"

    honest_R = trade["pnl"] / trade["risk"]
    assert result["expectancy_R"] == pytest.approx(honest_R, abs=1e-4)

    # Тот самый наивный расчёт: риск, пересчитанный по ОСТАВШЕМУСЯ объёму.
    remaining_share = 1 - settings.trading.partial_close_qty_pct / 100
    naive_R = trade["pnl"] / (trade["risk"] * remaining_share)
    assert naive_R > honest_R, "иначе тест ничего не ловит"
    assert naive_R == pytest.approx(honest_R / remaining_share)


def test_decision_metrics_are_reported(golden_db):
    """Движок обязан отдавать набор метрик решения, а не только PnL.

    Доверительный интервал на золотой фикстуре пуст — там одна сделка; сам
    bootstrap проверяется в tests/test_backtest_metrics.py.
    """
    result = simulate(_settings(), load_data(golden_db), has_oi=True)

    for key in ("expectancy_R", "total_R", "profit_factor",
                "max_drawdown_R", "worst_loss_streak", "return_pct_of_deposit"):
        assert key in result, key
    assert result["expectancy_R_ci"] is None, "одна сделка — интервала нет"
    assert result["expectancy_R_significant"] is False


# ---------------------------------------------------------------------------
# Шаг лота биржи
# ---------------------------------------------------------------------------
#
# Боевой путь округляет объём ВНИЗ (ccxt TRUNCATE) и отказывается от сделки,
# если после округления остался ноль или объём меньше минимального лота
# (PositionManager._place_market_entry → ExchangeConnector.amount_to_precision;
# 4 отказа "amount_too_small" по ZEC в боевой БД 27.08-21.09.2026). Движок до
# 22.09.2026 считал объём дробным, то есть систематически завышал размер.


class TestLotStep:
    def test_rounds_down_never_up(self):
        assert _round_to_lot(15.7, {"step": 10.0, "min_amount": 10.0}) == 10.0
        assert _round_to_lot(19.99, {"step": 10.0, "min_amount": 10.0}) == 10.0

    def test_below_min_amount_is_impossible(self):
        """Ровно случай ZEC: qty=0.0059 при минимуме 0.01 — сделки нет."""
        assert _round_to_lot(0.0059, {"step": 0.001, "min_amount": 0.01}) == 0.0

    def test_no_metadata_keeps_fractional_quantity(self):
        """Без метаданных поведение прежнее — иначе старые прогоны молча
        перестали бы сравниваться с новыми."""
        assert _round_to_lot(15.7, None) == 15.7

    def test_engine_skips_signal_when_lot_step_too_coarse(self, golden_db):
        """Шаг лота крупнее расчётного объёма → сделки нет, и это видно в
        отчёте отдельным счётчиком, а не молча."""
        data = load_data(golden_db)
        settings = _settings()

        without = simulate(settings, data, has_oi=True)
        assert without["trades"] == 1
        assert without["amount_too_small"] == 0

        coarse = {SYMBOL: {"step": 10.0 ** 9, "min_amount": 10.0 ** 9}}
        with_meta = simulate(settings, data, has_oi=True, markets=coarse)
        assert with_meta["trades"] == 0
        assert with_meta["amount_too_small"] == 1

    def test_engine_truncates_quantity_to_step(self, golden_db):
        """При проходимом шаге сделка остаётся, но объём урезан вниз, значит и
        PnL меньше — округление всегда против нас."""
        data = load_data(golden_db)
        settings = _settings()
        # Доля партиала ровно в один шаг лота: частичная фиксация остаётся
        # исполнимой, и обе ноги сделки масштабируются одинаково. Иначе тест
        # мерил бы сразу два эффекта — урезание объёма и отказ от партиала.
        settings.trading.partial_close_qty_pct = 50.0
        free = simulate(settings, data, has_oi=True)
        trade = free["trades_list"][0]

        # Объём, который посчитал движок без округления.
        qty = trade["risk"] / (trade["entry_price"] * settings.trading.stop_loss_pct / 100)
        # Шаг чуть меньше половины объёма: сделка остаётся возможной, но теряет
        # заметную долю размера.
        step = {SYMBOL: {"step": qty * 0.4, "min_amount": 0.0}}
        stepped = simulate(settings, data, has_oi=True, markets=step)

        assert stepped["trades"] == 1
        assert 0 < stepped["trades_list"][0]["pnl"] < trade["pnl"]
        # 2 шага из 2.5 → примерно 80% исходного размера.
        assert stepped["trades_list"][0]["pnl"] == pytest.approx(trade["pnl"] * 0.8, rel=0.05)

    def test_partial_below_lot_step_is_skipped_entirely(self, golden_db):
        """Доля партиала мельче шага лота → в бою лимитник не выставляется, и
        позиция идёт до TP/SL СО СВОИМ СТОПОМ: безубыток включает исполнение
        лимитника, а не достижение триггера.

        Это не то же самое, что partial_close_qty_pct = 0, где доля нулевая
        намеренно и безубыток как раз включается. Проверка нужна потому, что
        на депозите ~$55 нотионал позиции около $11, и доля 10-30% на монетах
        с грубым шагом недостижима — без этой модели свип на реальном депозите
        отвечал бы на вопрос, которого в бою не существует.
        """
        data = load_data(golden_db)
        settings = _settings()
        settings.trading.partial_close_qty_pct = 30.0
        free = simulate(settings, data, has_oi=True)

        qty = free["trades_list"][0]["risk"] / (
            free["trades_list"][0]["entry_price"] * settings.trading.stop_loss_pct / 100
        )
        # Объём усечётся до 0.8*qty, доля 30% от него — 0.24*qty, меньше шага.
        step = {SYMBOL: {"step": qty * 0.4, "min_amount": 0.0}}
        stepped = simulate(settings, data, has_oi=True, markets=step)

        assert stepped["trades"] == 1
        assert stepped["partial_unavailable"] == 1
        assert stepped["partials"] == 0
        assert free["partials"] == 1
        # Позиция дошла до TP целиком, поэтому на 80% размера заработала
        # БОЛЬШЕ, чем 80% от сделки с частичной фиксацией.
        assert stepped["trades_list"][0]["pnl"] > free["trades_list"][0]["pnl"] * 0.8

    def test_partial_qty_zero_still_moves_stop_to_breakeven(self, golden_db):
        """Нулевая доля — намеренная конфигурация, а не отказ биржи: безубыток
        работает. Граница с предыдущим тестом держится этим."""
        data = load_data(golden_db)
        settings = _settings()
        settings.trading.partial_close_qty_pct = 0.0
        result = simulate(settings, data, has_oi=True,
                          markets={SYMBOL: {"step": 0.001, "min_amount": 0.0}})

        assert result["partial_unavailable"] == 0
        assert result["partials"] == 1


def test_symbol_missing_from_lot_metadata_is_reported(golden_db):
    """Монета, которой нет в метаданных, торгуется дробным объёмом — но это
    попадает в отчёт. Устаревший файл метаданных иначе молча вернул бы
    поведение до 22.09.2026."""
    data = load_data(golden_db)
    result = simulate(_settings(), data, has_oi=True,
                      markets={"ДРУГАЯ/USDT:USDT": {"step": 1.0, "min_amount": 1.0}})

    assert result["trades"] == 1, "сделка состоялась, объём не округлялся"
    assert result["symbols_without_lot_meta"] == [SYMBOL]


# ---------------------------------------------------------------------------
# Векторный предфильтр по объёму
# ---------------------------------------------------------------------------
#
# Предфильтр обязан быть НАДМНОЖЕСТВОМ гейта детектора: он отсекает только те
# бары, на которых check_volume_pattern гарантированно вернёт False. Если он
# съест хотя бы одного настоящего кандидата, результат бэктеста поедет молча —
# ровно тот класс регрессий, из-за которого в проекте появилось правило 3.


def _noisy_market_db(path, n_symbols: int = 6, n_bars: int = 420, seed: int = 20260922):
    """Случайные монеты со всплесками объёма разной силы, в т.ч. пограничными.

    Смысл фикстуры — не «реалистичный рынок», а множество баров рядом с порогом
    всплеска: именно там ошибка выравнивания предфильтра на один бар и вылезет.
    Часть всплесков сопровождается ростом цены и доходит до сигнала, часть —
    нет; и то и другое полезно, лишь бы сигналы вообще были.
    """
    import random

    rnd = random.Random(seed)
    burst_mults = [3.0, 4.6, 4.9, 5.1, 5.4, 8.0]
    candles, oi = [], []
    for s_i in range(n_symbols):
        symbol = f"SYM{s_i}/USDT:USDT"
        price = 10.0 * (s_i + 1)
        burst_at = set()
        for start in range(90, n_bars - 40, 47):
            burst_at.update({start, start + 1, start + 2, start + 3})
        for i in range(n_bars):
            vol = rnd.uniform(900, 1100)
            if i in burst_at:
                vol *= burst_mults[(i + s_i) % len(burst_mults)]
                price *= 1.006           # ~2.4% за четыре бара окна
            elif (i + s_i) % 37 == 0:
                vol *= rnd.choice(burst_mults)   # всплеск объёма без роста цены
                price *= 1 + rnd.uniform(-0.0008, 0.0008)
            else:
                price *= 1 + rnd.uniform(-0.0008, 0.0008)
            candles.append(("bybit", symbol, _ts(i), price, price * 1.002,
                            price * 0.998, price, vol))
            oi.append(("bybit", symbol, _ts(i), 1_000_000.0 + i * 100))
    _write_db(path, candles, oi)


class TestVolumeGatePrefilter:
    def test_results_are_identical_with_and_without(self, tmp_path):
        """Главная проверка: посделочный список обязан совпасть целиком."""
        db = tmp_path / "noisy.db"
        _noisy_market_db(db)
        data = load_data(str(db))
        settings = _settings()

        slow = simulate(settings, data, has_oi=True, prefilter=False)
        fast = simulate(settings, data, has_oi=True, prefilter=True)

        assert slow["signals"] > 0, "фикстура обязана давать сигналы, иначе тест пустой"
        assert fast["trades_list"] == slow["trades_list"]
        assert fast["signals"] == slow["signals"]
        assert fast["total_pnl"] == slow["total_pnl"]
        assert fast["prefiltered"] > 0, "предфильтр обязан хоть что-то отсекать"

    def test_identical_across_strategy_thresholds(self, tmp_path):
        """То же на других порогах: маска строится по конфигу, и сдвиг
        baseline_bars/sustain_bars не должен её ломать."""
        db = tmp_path / "noisy.db"
        _noisy_market_db(db)
        data = load_data(str(db))

        for mult, min_usdt in ((3.0, 0.0), (5.0, 0.0), (5.0, 5_000.0), (7.0, 0.0)):
            settings = _settings()
            settings.strategy.volume_surge_mult = mult
            settings.strategy.min_baseline_volume_usdt = min_usdt
            slow = simulate(settings, data, has_oi=True, prefilter=False)
            fast = simulate(settings, data, has_oi=True, prefilter=True)
            assert fast["trades_list"] == slow["trades_list"], (mult, min_usdt)

    def test_mask_admits_every_bar_the_detector_would_accept(self, tmp_path):
        """Прямая проверка надмножества: на каждом баре, где детектор сказал бы
        «да», маска обязана быть True."""
        db = tmp_path / "noisy.db"
        _noisy_market_db(db)
        data = load_data(str(db))
        settings = _settings()
        detector = SetupDetector(settings.strategy,
                                 timeframe=settings.collectors.timeframe)
        mask = build_volume_gate_mask(data["symbols"], detector.config)

        need = settings.strategy.baseline_bars + settings.strategy.sustain_bars
        checked = accepted = 0
        for sym, rows in data["symbols"].items():
            for i in range(need, len(rows)):
                candle_slice = [
                    {"open": r[1], "high": r[2], "low": r[3],
                     "close": r[4], "volume": r[5]}
                    for r in rows[max(0, i - need - 9):i + 1]
                ]
                if len(candle_slice) < need:
                    continue
                checked += 1
                for window in (candle_slice, candle_slice[:-1]):
                    if len(window) >= need and detector.check_volume_pattern(window, {}):
                        accepted += 1
                        assert mask[sym][i], f"{sym} бар {i}: маска съела кандидата"
                        break
        assert checked > 0 and accepted > 0, (checked, accepted)


def test_engine_calls_detector_shift_retry_instead_of_copying_it(golden_db, monkeypatch):
    """Движок обязан ЗВАТЬ SetupDetector._match_volume_window, а не повторять его.

    До 22.09.2026 в движке лежала копия shift-ретрая, совпадавшая с оригиналом
    построчно. Именно поэтому она не узнала бы про новый флаг стратегии
    shift_retry_on_undersized_bar и молча считала бы по-старому — ровно тот
    класс расхождений, ради которого написано правило 2 AGENTS.md.
    """
    calls = []
    original = SetupDetector._match_volume_window

    def spy(self, candles, min_bars, context, symbol=None):
        calls.append(len(candles))
        return original(self, candles, min_bars, context, symbol)

    monkeypatch.setattr(SetupDetector, "_match_volume_window", spy)
    result = simulate(_settings(), load_data(golden_db), has_oi=True, prefilter=False)

    assert calls, "движок не позвал общий метод детектора"
    assert result["trades"] == 1, "поведение при этом не изменилось"


def test_maturity_threshold_does_not_touch_the_backtest(golden_db):
    """Порог зрелости бара не должен менять бэктест ни при каком значении.

    В бэктесте бары закрыты, возраст последнего неизвестен, и вердикт «свеча
    слишком маленькая» там честный. Замер 22.09.2026: грубое расширение
    ретрая (без условия на зрелость) стоило -2.30R — этот тест и стоит на том,
    чтобы правка не протекла в движок.
    """
    data = load_data(golden_db)
    base = simulate(_settings(), data, has_oi=True)
    for pct in (0.0, 50.0, 80.0, 100.0):
        s = _settings()
        s.strategy.undersized_verdict_min_bar_maturity_pct = pct
        assert simulate(s, data, has_oi=True)["trades_list"] == base["trades_list"], pct


def test_deposit_scales_result_without_lot_metadata(golden_db):
    """Без метаданных инструментов депозит — чистый масштаб: R не меняется."""
    data = load_data(golden_db)
    small = _settings()
    small.trading.backtest_deposit_usdt = 55.0
    big = _settings()
    big.trading.backtest_deposit_usdt = 1000.0

    rs, rb = simulate(small, data, has_oi=True), simulate(big, data, has_oi=True)
    assert rs["expectancy_R"] == pytest.approx(rb["expectancy_R"])
    # abs=0.01: total_pnl округляется до цента, точнее сравнивать нечего.
    assert rs["total_pnl"] == pytest.approx(rb["total_pnl"] * 55.0 / 1000.0, abs=0.01)


def test_small_deposit_feels_the_lot_step(golden_db):
    """С метаданными депозит перестаёт быть масштабом: чем он меньше, тем
    большую долю объёма съедает округление вниз.

    Ровно поэтому прогон на виртуальном $1000 при реальном счёте $55
    недооценивает шаг лота — замер 22.09.2026 дал 0.22% против 3.2%.
    """
    data = load_data(golden_db)
    trade = simulate(_settings(), data, has_oi=True)["trades_list"][0]
    qty_at_1000 = trade["risk"] / (trade["entry_price"] * 5.0 / 100)
    # Шаг лота крупный относительно объёма на маленьком депозите и мелкий на большом.
    markets = {SYMBOL: {"step": qty_at_1000 * 55.0 / 1000.0 * 0.4, "min_amount": 0.0}}

    small = _settings()
    small.trading.backtest_deposit_usdt = 55.0
    big = _settings()
    big.trading.backtest_deposit_usdt = 1000.0

    rs = simulate(small, data, has_oi=True, markets=markets)
    rb = simulate(big, data, has_oi=True, markets=markets)
    assert rs["expectancy_R"] < rb["expectancy_R"], (rs["expectancy_R"], rb["expectancy_R"])
