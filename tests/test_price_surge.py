"""
Тесты PriceSurgeDetector — порог роста и кулдаун на повторные сигналы.

Кулдаун читает уже отправленные сигналы из БД, поэтому тесты идут на
in-memory SQLite, а не на чистых функциях.
"""

from datetime import datetime, timedelta, timezone

import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.analytics.data_provider import DataProvider
from src.analytics.price_surge import PriceSurgeDetector
from src.config import StrategyConfig
from src.storage.models import Base, Candle, PriceSurgeSignal, Ticker

# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session():
    """Сессия к in-memory БД со всеми таблицами."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as sess:
        yield sess
    await engine.dispose()


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------

NOW = datetime.now(tz=timezone.utc).replace(microsecond=0)


def _detector(**overrides) -> PriceSurgeDetector:
    params = {
        "price_surge_pct": 5.0,
        "price_surge_minutes": 18,
        "price_surge_cooldown_minutes": 60,
        "price_surge_escalation_pct": 0.0,
    }
    params.update(overrides)
    return PriceSurgeDetector(StrategyConfig(**params), timeframe="3m")


async def _seed_pump(
    session: AsyncSession,
    symbol: str,
    growth_pct: float,
    exchange: str = "bybit",
) -> None:
    """Ровный рост на growth_pct ровно за окно детектора (6 свечей по 3м = 18 мин).

    Перед окном — столько же плоских свечей: детектор требует минимум
    window_bars + 1 баров. `get_active_symbols` берёт только символы, которые
    есть в тикерах ByBit, поэтому тикер тоже нужен.
    """
    exists = await session.scalar(
        select(Ticker.id).where(Ticker.exchange == "bybit", Ticker.symbol == symbol)
    )
    if exists is None:
        session.add(Ticker(exchange="bybit", symbol=symbol, timestamp=NOW, last=1.0))

    window_bars = 6
    flat_bars = 6
    step = (1 + growth_pct / 100) ** (1 / window_bars)
    price = 1.0
    total = flat_bars + window_bars
    for i in range(total):
        nxt = price if i < flat_bars else price * step
        session.add(
            Candle(
                exchange=exchange,
                symbol=symbol,
                timestamp=NOW - timedelta(minutes=3 * (total - i)),
                open=price, high=max(price, nxt), low=min(price, nxt),
                close=nxt, volume=1000.0,
            )
        )
        price = nxt
    await session.flush()


async def _alert(session: AsyncSession, symbol: str, change_pct: float, ago_min: float) -> None:
    """Записать уже отправленный сигнал — как это делает PriceSurgeSignalProcessor."""
    session.add(
        PriceSurgeSignal(
            timestamp=NOW - timedelta(minutes=ago_min),
            symbol=symbol,
            change_pct=change_pct,
            interval_minutes=18,
        )
    )
    await session.flush()


def _dp() -> DataProvider:
    """Без персистентного кеша — тестам нужен прямой путь в БД."""
    return DataProvider()


# ---------------------------------------------------------------------------
# Порог
# ---------------------------------------------------------------------------


class TestThreshold:
    async def test_pump_above_threshold_signals(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=8.0)
        det = _detector()
        det.data_provider = _dp()
        signals = await det.analyze(session)

        assert [s.symbol for s in signals] == ["PUMP/USDT"]

    async def test_growth_below_threshold_ignored(self, session):
        await _seed_pump(session, "FLAT/USDT", growth_pct=2.0)
        det = _detector()
        det.data_provider = _dp()

        assert await det.analyze(session) == []

    async def test_disabled_when_pct_zero(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=20.0)
        det = _detector(price_surge_pct=0.0)
        det.data_provider = _dp()

        assert await det.analyze(session) == []


# ---------------------------------------------------------------------------
# Кулдаун
# ---------------------------------------------------------------------------


class TestCooldown:
    async def test_recent_alert_suppresses_repeat(self, session):
        """Тот же памп на следующем цикле — без повторного сообщения."""
        await _seed_pump(session, "PUMP/USDT", growth_pct=8.0)
        await _alert(session, "PUMP/USDT", change_pct=8.0, ago_min=1)
        det = _detector()
        det.data_provider = _dp()

        assert await det.analyze(session) == []

    async def test_alert_outside_window_does_not_suppress(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=8.0)
        await _alert(session, "PUMP/USDT", change_pct=8.0, ago_min=61)
        det = _detector()
        det.data_provider = _dp()

        assert [s.symbol for s in await det.analyze(session)] == ["PUMP/USDT"]

    async def test_other_symbol_not_suppressed(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=8.0)
        await _alert(session, "OTHER/USDT", change_pct=8.0, ago_min=1)
        det = _detector()
        det.data_provider = _dp()

        assert [s.symbol for s in await det.analyze(session)] == ["PUMP/USDT"]

    async def test_cooldown_off_repeats_every_cycle(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=8.0)
        await _alert(session, "PUMP/USDT", change_pct=8.0, ago_min=1)
        det = _detector(price_surge_cooldown_minutes=0)
        det.data_provider = _dp()

        assert [s.symbol for s in await det.analyze(session)] == ["PUMP/USDT"]

    async def test_escalation_breaks_cooldown(self, session):
        """Памп разогнался на +5 п.п. сверх отправленного — сообщаем снова."""
        await _seed_pump(session, "PUMP/USDT", growth_pct=14.0)
        await _alert(session, "PUMP/USDT", change_pct=8.0, ago_min=1)
        det = _detector(price_surge_escalation_pct=5.0)
        det.data_provider = _dp()

        assert [s.symbol for s in await det.analyze(session)] == ["PUMP/USDT"]

    async def test_escalation_below_step_still_suppressed(self, session):
        await _seed_pump(session, "PUMP/USDT", growth_pct=10.0)
        await _alert(session, "PUMP/USDT", change_pct=8.0, ago_min=1)
        det = _detector(price_surge_escalation_pct=5.0)
        det.data_provider = _dp()

        assert await det.analyze(session) == []

    async def test_escalation_measured_from_window_max(self, session):
        """Планка — максимум в окне, а не последнее сообщение: «пила» не пробивает."""
        await _seed_pump(session, "PUMP/USDT", growth_pct=12.0)
        await _alert(session, "PUMP/USDT", change_pct=11.0, ago_min=10)
        await _alert(session, "PUMP/USDT", change_pct=6.0, ago_min=1)
        det = _detector(price_surge_escalation_pct=5.0)
        det.data_provider = _dp()

        assert await det.analyze(session) == []


# ---------------------------------------------------------------------------
# Дедуп внутри цикла
# ---------------------------------------------------------------------------


class TestCrossExchangeDedup:
    async def test_same_symbol_on_two_exchanges_gives_one_signal(self, session):
        """Монета есть и на binance, и на bybit — сообщение должно быть одно."""
        await _seed_pump(session, "DUAL/USDT", growth_pct=8.0, exchange="bybit")
        await _seed_pump(session, "DUAL/USDT", growth_pct=12.0, exchange="binance")
        det = _detector()
        det.data_provider = _dp()

        signals = await det.analyze(session)

        assert len(signals) == 1
        # Берём максимальный рост из двух
        assert "+12" in signals[0].message
