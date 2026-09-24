"""Политика таймаутов и ретраев: цикл сбора против торговых вызовов.

Регресс, ради которого это написано: у сбора и торговли противоположная цена
ошибки, а политика была одна на всех — 30 с сокетного таймаута и три попытки с
backoff'ом. Для торговли это верно, для сбора разорительно: попытка держит слот
семафора, и одна зависшая монета съедает 3 x 30 = 90 слото-секунд при
латентности здорового запроса 0.33 с. Аудит боевой БД (27.08-01.09.2026) видел
из-за этого циклы по 800-1600 с вместо 78 с.
"""

import asyncio
import time

import ccxt
import pytest

from src.connectors import exchange as ex_mod
from src.connectors.exchange import ExchangeConnector


class _FakeCcxt:
    """Заглушка объекта ccxt: считает вызовы, умеет висеть и падать."""

    def __init__(self, behaviour, hang_sec: float = 5.0):
        self.calls = 0
        self._behaviour = behaviour
        self._hang = hang_sec
        self.apiKey = ""

    def some_method(self, *args, **kwargs):
        self.calls += 1
        if self._behaviour == "hang":
            time.sleep(self._hang)
            return "поздно"
        if self._behaviour == "network_error":
            raise ccxt.NetworkError("сеть отвалилась")
        return "ок"


def _connector(behaviour: str, hang_sec: float = 5.0) -> ExchangeConnector:
    c = ExchangeConnector("bybit", concurrency=5)
    c._exchange = _FakeCcxt(behaviour, hang_sec)
    return c


async def test_scan_call_gives_up_fast_and_never_retries(monkeypatch):
    """Зависший вызов сбора обязан отпустить слот по дедлайну и не повторяться."""
    monkeypatch.setattr(ex_mod, "SCAN_CALL_TIMEOUT_SEC", 0.2)
    c = _connector("hang", hang_sec=1.0)

    t = time.perf_counter()
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        await c._call("some_method", scan=True)
    elapsed = time.perf_counter() - t

    assert elapsed < 1.0, f"scan-вызов ждал {elapsed:.2f}с вместо дедлайна 0.2с"
    assert c._exchange.calls == 1, "scan-вызов не должен ретраиться"


async def test_scan_timeout_releases_the_semaphore_slot(monkeypatch):
    """Слот семафора должен освобождаться по дедлайну, а не по сокетному
    таймауту ccxt: иначе зависшая монета продолжает занимать место в очереди."""
    monkeypatch.setattr(ex_mod, "SCAN_CALL_TIMEOUT_SEC", 0.2)
    c = _connector("hang", hang_sec=1.0)
    c._semaphore = asyncio.Semaphore(1)  # один слот на всех

    async def one():
        try:
            await c._call("some_method", scan=True)
        except (asyncio.TimeoutError, TimeoutError):
            pass

    t = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(4)))
    elapsed = time.perf_counter() - t

    # 4 вызова по 0.2с дедлайна = ~0.8с. Если бы слот держался до конца
    # висящего потока (1с), вышло бы ~4с.
    assert elapsed < 2.0, (
        f"4 зависших вызова через 1 слот заняли {elapsed:.2f}с — "
        f"похоже, слот держится до сокетного таймаута, а не до дедлайна"
    )


async def test_trading_call_still_retries(monkeypatch):
    """Торговый путьне тронут: ордер терять нельзя."""
    monkeypatch.setattr(ex_mod, "RETRY_DELAY", 0)
    c = _connector("network_error")

    with pytest.raises(ccxt.NetworkError):
        await c._call("some_method")

    assert c._exchange.calls == ex_mod.MAX_RETRIES, (
        f"торговый вызов сделал {c._exchange.calls} попыток, "
        f"ожидалось {ex_mod.MAX_RETRIES}"
    )


class _BadSymbolCcxt(_FakeCcxt):
    def fetch_ohlcv(self, *args, **kwargs):
        self.calls += 1
        raise ccxt.BadSymbol("bybit does not have market symbol ACX/USDT:USDT")


async def test_missing_symbol_is_remembered_once():
    """Монеты, которых на бирже нет, живут в tickers и каждый цикл отдают
    ошибку (замер 01.09.2026: ACX/USDT:USDT на bybit). Ошибка мгновенная,
    но и спрашивать про неё каждый цикл незачем."""
    c = ExchangeConnector("bybit", concurrency=5)
    c._exchange = _BadSymbolCcxt("ok")

    with pytest.raises(ccxt.BadSymbol):
        await c.fetch_ohlcv("ACX/USDT:USDT")

    assert "ACX/USDT:USDT" in c.unsupported_symbols


async def test_collector_skips_unsupported_symbols():
    from src.collectors.market_data import MarketDataCollector

    collector = MarketDataCollector(
        connectors=[], exclude_coins=[], min_volume_usdt=0.0,
        interval_seconds=15, timeframe="3m",
    )

    class _Conn:
        exchange_id = "bybit"
        unsupported_symbols = {"ACX/USDT:USDT"}

    selected = [{"symbol": "ACX/USDT:USDT"}, {"symbol": "SUI/USDT:USDT"}]
    kept = collector._supported(_Conn(), selected)

    assert [t["symbol"] for t in kept] == ["SUI/USDT:USDT"]


def test_connection_pool_matches_concurrency():
    """Регресс деплоя 01.09.2026: подняли concurrency до 20, а пул соединений
    `requests.Session` остался дефолтным (10). Половина запросов не находила
    свободного соединения — urllib3 создавал новое, отдавал ответ и выбрасывал
    его, платя полный TCP+TLS handshake, и заливал лог сотнями строк
    «Connection pool is full, discarding connection» за цикл.
    """
    c = ExchangeConnector("bybit", concurrency=20)
    adapter = c._exchange.session.get_adapter("https://api.bybit.com")

    assert adapter._pool_maxsize > 20, (
        f"пул соединений {adapter._pool_maxsize} без запаса над конкурентностью 20: "
        f"зависшие по дедлайну потоки держат соединения до FETCH_TIMEOUT, и "
        f"следующая партия снова упрётся в полный пул"
    )
    assert adapter._pool_connections > 20


# ---------------------------------------------------------------------------
# Режим позиции: ByBit retCode 10001 "position idx not match position mode"
# ---------------------------------------------------------------------------
#
# Регресс: бот не передаёт positionIdx и не выставлял режим позиции, поэтому на
# символе с включённым hedge-режимом ордер отклонялся. Поймано на DASH — 3
# сигнала 27.08-21.09.2026 не дошли до ордера, после третьего символ ушёл в
# error-cooldown на 4 часа (аудит 22.09.2026).


class _PositionModeCcxt(_FakeCcxt):
    """Считает вызовы set_position_mode и умеет отвечать как ByBit."""

    def __init__(self, error: Exception | None = None):
        super().__init__("ok")
        self.mode_calls: list[tuple] = []
        self._error = error

    def set_position_mode(self, hedged, symbol=None, params=None):
        self.mode_calls.append((hedged, symbol))
        if self._error is not None:
            raise self._error
        return {"retCode": 0}


def _mode_connector(fake: _PositionModeCcxt) -> ExchangeConnector:
    conn = ExchangeConnector.__new__(ExchangeConnector)
    conn._exchange = fake
    conn.exchange_id = "bybit"
    conn._semaphore = asyncio.Semaphore(4)
    conn.unsupported_symbols = set()
    conn._one_way_symbols = set()
    return conn


async def test_one_way_mode_set_once_per_symbol():
    """Режим выставляется при первом входе и больше не дёргается: лишний
    приватный вызов на каждую сделку не нужен."""
    fake = _PositionModeCcxt()
    conn = _mode_connector(fake)

    await conn.ensure_one_way_mode("DASH/USDT:USDT")
    await conn.ensure_one_way_mode("DASH/USDT:USDT")
    await conn.ensure_one_way_mode("ETH/USDT:USDT")

    assert fake.mode_calls == [
        (False, "DASH/USDT:USDT"),
        (False, "ETH/USDT:USDT"),
    ]


async def test_already_one_way_is_not_an_error():
    """retCode 110025 — это «режим уже такой», а не отказ: символ кешируется,
    повторных вызовов не будет."""
    fake = _PositionModeCcxt(error=ccxt.ExchangeError('bybit {"retCode":110025,'
                                                      '"retMsg":"Position mode is not modified"}'))
    conn = _mode_connector(fake)

    await conn.ensure_one_way_mode("DASH/USDT:USDT")
    await conn.ensure_one_way_mode("DASH/USDT:USDT")

    assert len(fake.mode_calls) == 1


async def test_real_failure_is_swallowed_and_retried_next_time():
    """Настоящий отказ наружу не выпускаем — ордер всё равно стоит попробовать,
    он провалится не хуже, чем раньше. Но символ не кешируем: следующий вход
    попытается снова. Ретраев при этом НЕ будет: _call повторяет только сетевые
    ошибки, а ExchangeError пробрасывает сразу — иначе постоянный отказ стоил бы
    трёх приватных вызовов на каждый вход."""
    fake = _PositionModeCcxt(error=ccxt.ExchangeError("что-то совсем другое"))
    conn = _mode_connector(fake)

    await conn.ensure_one_way_mode("DASH/USDT:USDT")  # не должно бросить
    await conn.ensure_one_way_mode("DASH/USDT:USDT")

    assert len(fake.mode_calls) == 2
    assert conn._one_way_symbols == set()


# Плечо выше биржевого максимума. 24.09.2026 сигнал TUT не открылся: ByBit
# снизил TUT максимум до 5x, на символе осталось 10x, set_leverage(10) ответил
# "leverage not modified", а ордер — 110013 "cannot set leverage [1000] gt
# maxLeverage [500] by risk limit".


class _LeverageCcxt(_FakeCcxt):
    def __init__(self, tiers=None, tiers_error: Exception | None = None):
        super().__init__("ok")
        self.leverage_calls: list[tuple] = []
        self._tiers = tiers
        self._tiers_error = tiers_error

    def fetch_market_leverage_tiers(self, symbol, params=None):
        if self._tiers_error is not None:
            raise self._tiers_error
        return self._tiers

    def set_leverage(self, leverage, symbol=None, params=None):
        self.leverage_calls.append((leverage, symbol))
        return {"retCode": 0}


async def test_leverage_clamped_to_exchange_max():
    fake = _LeverageCcxt(tiers=[{"tier": 1, "maxLeverage": 5.0},
                                {"tier": 2, "maxLeverage": 4.9}])
    conn = _mode_connector(fake)

    await conn.set_leverage("TUT/USDT:USDT", 10)

    assert fake.leverage_calls == [(5.0, "TUT/USDT:USDT")]


async def test_leverage_below_max_untouched():
    fake = _LeverageCcxt(tiers=[{"tier": 1, "maxLeverage": 50.0}])
    conn = _mode_connector(fake)

    await conn.set_leverage("ETH/USDT:USDT", 10)

    assert fake.leverage_calls == [(10, "ETH/USDT:USDT")]


async def test_leverage_falls_back_to_config_when_tiers_unavailable():
    """Не узнали максимум — ставим как в конфиге, а не отказываемся от входа."""
    fake = _LeverageCcxt(tiers_error=ccxt.ExchangeError("недоступно"))
    conn = _mode_connector(fake)

    await conn.set_leverage("ETH/USDT:USDT", 10)

    assert fake.leverage_calls == [(10, "ETH/USDT:USDT")]
