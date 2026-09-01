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
