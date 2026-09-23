from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from src.backtest.metrics import BREAKEVEN, breakeven_credit, outcome, outcome_counts
from src.storage.models import Trade


async def trade_stats(session, period: str = "all", trading=None) -> str:
    """Торговая статистика за период: day, week, month, all.

    `trading` — конфиг сделок: нужен для веса безубытка во взвешенном
    winrate. Без него строка со взвешенным winrate просто не появится.
    """
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

    total_pnl = sum(t.pnl or 0 for t in trades)
    # Безубыток — отдельный класс, а не «плюс»: сделку, снятую с безубыточного
    # стопа после частичной фиксации, приносит только забронированная на
    # триггере часть (при доле 20% и RR 2.0 это +0.14R против +2R у полного
    # тейка), и рынок при этом вернулся к цене входа. Классификация общая с
    # бэктестом — `metrics.outcome`, чтобы отчёты не расходились.
    counts = outcome_counts(
        [{"entry_price": t.entry_price, "exit_price": t.exit_price,
          "pnl": t.pnl or 0.0, "direction": t.direction}
         for t in trades],
        be_credit=(breakeven_credit(trading.risk_reward_ratio, trading.partial_close_pct,
                                    trading.partial_close_qty_pct) if trading else None),
    )
    be_pnl = sum(t.pnl or 0 for t in trades
                 if outcome({"entry_price": t.entry_price, "exit_price": t.exit_price,
                             "pnl": t.pnl or 0.0, "direction": t.direction}) == BREAKEVEN)

    return (
        f"📊 <b>Статистика за {labels[period]}</b>\n\n"
        f"Сделок: {len(trades)}\n"
        f"Тейк: {counts['wins']} | Безубыток: {counts['breakevens']} | "
        f"Стоп: {counts['losses']}\n"
        f"Win rate: {counts['win_rate']:.0f}% (безубытки не в счёт)\n"
        + (f"Win rate взвеш.: {counts['win_rate_weighted']:.1f}% "
           f"(безубыток = 1/{1/counts['breakeven_credit']:.1f} тейка, "
           f"зачтено {counts['breakeven_equivalent_wins']:.1f})\n"
           if counts.get("breakeven_credit") else "")
        + f"PnL: ${total_pnl:+.2f}"
        + (f", из него с безубытков ${be_pnl:+.2f}\n" if counts["breakevens"] else "\n")
        + f"\nОткрыто позиций: {open_count}{pending_line}"
    )
