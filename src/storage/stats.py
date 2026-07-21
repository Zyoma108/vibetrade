from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from src.storage.models import Trade


async def trade_stats(session, period: str = "all") -> str:
    """Торговая статистика за период: day, week, month, all."""
    now = datetime.now(tz=timezone.utc)
    periods = {
        "day": now - timedelta(days=1),
        "week": now - timedelta(days=7),
        "month": now - timedelta(days=30),
        "all": None,
    }
    since = periods.get(period)
    labels = {"day": "24 часа", "week": "7 дней", "month": "30 дней", "all": "Всё время"}

    # Закрытые сделки
    stmt = select(Trade).where(Trade.status == "closed")
    if since:
        stmt = stmt.where(Trade.exit_time >= since)
    stmt = stmt.order_by(Trade.exit_time)
    result = await session.execute(stmt)
    trades = result.scalars().all()

    # Открытые позиции и pending-заявки на вход
    open_stmt = select(func.count()).select_from(Trade).where(Trade.status == "open")
    open_count = (await session.execute(open_stmt)).scalar() or 0
    pending_stmt = select(func.count()).select_from(Trade).where(Trade.status == "pending")
    pending_count = (await session.execute(pending_stmt)).scalar() or 0
    pending_line = f"\nЛимитников на вход: {pending_count}" if pending_count else ""

    if not trades:
        return (
            f"📊 <b>Статистика за {labels[period]}</b>\n\n"
            f"Закрытых сделок: 0\n"
            f"Открыто позиций: {open_count}{pending_line}"
        )

    wins = sum(1 for t in trades if (t.pnl or 0) > 0)
    losses = sum(1 for t in trades if (t.pnl or 0) <= 0)
    total_pnl = sum(t.pnl or 0 for t in trades)
    win_rate = wins / len(trades) * 100 if trades else 0

    return (
        f"📊 <b>Статистика за {labels[period]}</b>\n\n"
        f"Сделок: {len(trades)} | Плюс: {wins} | Минус: {losses}\n"
        f"Win rate: {win_rate:.0f}%\n"
        f"PnL: ${total_pnl:+.2f}\n\n"
        f"Открыто позиций: {open_count}{pending_line}"
    )
