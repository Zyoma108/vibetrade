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

from src.backtest.engine import load_data, simulate
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
