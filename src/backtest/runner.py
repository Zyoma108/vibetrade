"""
Прогон стратегии на исторических данных: отчёт поверх `src/backtest/engine.py`.

Сам цикл симуляции живёт в движке и здесь НЕ дублируется — см. `engine.py`
про то, почему реализаций было три и чем это обошлось. Здесь только загрузка
конфига, сравнение с реальными сделками из той же БД и печать.

Использование:
    python -m src.backtest.runner
    python -m src.backtest.runner --db data/trading_bot.db --config config/config.yaml
"""

import argparse
import logging
from pathlib import Path

from src.backtest.engine import load_data, simulate
from src.config import Settings

logger = logging.getLogger(__name__)


def run_backtest(config_path: str, db_path: str, has_oi: bool = True) -> dict | None:
    """Загрузить БД и прогнать симуляцию. Тонкая обёртка над движком."""
    settings = Settings.from_yaml(config_path)
    data = load_data(db_path)
    if not data["all_timestamps"]:
        return None
    return simulate(settings, data, has_oi=has_oi)


def _load_live_stats(db_path: str) -> dict | None:
    """Загрузить статистику реальных сделок из БД."""
    import sqlite3

    try:
        db = sqlite3.connect(db_path)
    except Exception:
        return None

    trades = db.execute(
        "SELECT symbol, direction, entry_price, exit_price, "
        "entry_time, exit_time, pnl, status, partial_closed, partial_pnl "
        "FROM trades WHERE status = 'closed' ORDER BY exit_time"
    ).fetchall()

    if not trades:
        db.close()
        return None

    wins = sum(1 for t in trades if (t[6] or 0) > 0)
    losses = sum(1 for t in trades if (t[6] or 0) <= 0)
    total_pnl = sum(t[6] or 0 for t in trades)
    win_rate = wins / len(trades) * 100 if trades else 0

    # Диапазон дат
    entry_times = [t[4] for t in trades if t[4]]
    exit_times = [t[5] for t in trades if t[5]]
    period = ""
    if entry_times and exit_times:
        period = f"{min(entry_times)[:19]} → {max(exit_times)[:19]}"

    # Причины выхода (по данным сигналов: tp, sl, time)
    signals = db.execute(
        "SELECT s.symbol, s.timestamp, s.missed_reason "
        "FROM signals s ORDER BY s.timestamp"
    ).fetchall()

    sent_count = sum(1 for s in signals if s[2] is None)
    missed_count = sum(1 for s in signals if s[2] is not None)
    missed_reasons = {}
    for s in signals:
        if s[2]:
            missed_reasons[s[2]] = missed_reasons.get(s[2], 0) + 1

    db.close()

    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(total_pnl / len(trades), 2) if trades else 0,
        "period": period,
        "signals_total": len(signals),
        "signals_sent": sent_count,
        "signals_missed": missed_count,
        "missed_reasons": missed_reasons,
        "best": max(trades, key=lambda t: t[6] or 0) if trades else None,
        "worst": min(trades, key=lambda t: t[6] or 0) if trades else None,
    }


def _print_comparison(bt: dict, live: dict | None) -> None:
    """Вывести сравнение бэктест ↔ реальная торговля."""
    print("\n" + "=" * 80)
    print(f"  {'':30s} {'БЭКТЕСТ':>22s} {'РЕАЛЬНАЯ ТОРГОВЛЯ':>22s}")
    print("=" * 80)

    rows = [
        ("Сделок", str(bt["trades"]), str(live["trades"]) if live else "—"),
        ("Плюс / Минус",
         f"{bt['wins']} / {bt['losses']}",
         f"{live['wins']} / {live['losses']}" if live else "—"),
        ("Win rate",
         f"{bt['win_rate']}%",
         f"{live['win_rate']}%" if live else "—"),
        ("Total PnL",
         f"${bt['total_pnl']:+.2f}",
         f"${live['total_pnl']:+.2f}" if live else "—"),
        ("Средний PnL",
         f"${bt['avg_pnl']:+.2f}",
         f"${live['avg_pnl']:+.2f}" if live else "—"),
        ("TP / SL / Time",
         f"{bt['tp_wins']} / {bt['sl_losses']} / {bt['time_exits']}",
         "—"),
        ("Частичных закр.",
         str(bt["partials"]),
         "—"),
        ("Период",
         bt["period"][:35] if len(bt["period"]) > 35 else bt["period"],
         live["period"][:35] if live and live["period"] and len(live["period"]) > 35 else (live["period"] if live else "—")),
    ]

    for label, bt_val, live_val in rows:
        print(f"  {label:<30s} {bt_val:>22s} {live_val:>22s}")

    print("=" * 80)

    if live and live.get("signals_total"):
        print("\n  Конвейер сигналов (реальная торговля):")
        print(f"    Всего сигналов: {live['signals_total']}")
        print(f"    Отправлено:     {live['signals_sent']}")
        print(f"    Пропущено:      {live['signals_missed']}")
        if live["missed_reasons"]:
            for reason, count in sorted(
                live["missed_reasons"].items(), key=lambda x: -x[1]
            ):
                print(f"      - {reason}: {count}")


def _print_trade_list(trades: list, title: str, max_show: int = 20) -> None:
    """Вывести список сделок."""
    print(f"\n  {title} (последние {min(len(trades), max_show)}):")
    for t in trades[-max_show:]:
        emoji = "✅" if t["exit_reason"] == "tp" else (
            "🛑" if t["exit_reason"] == "sl" else "⏰"
        )
        pnl_pct = (t["exit_price"] / t["entry_price"] - 1) * 100
        print(
            f"  {emoji} {t['symbol']:25s} "
            f"вход=${t['entry_price']:.6f} выход=${t['exit_price']:.6f} "
            f"PnL=${t['pnl']:+.2f} ({pnl_pct:+.1f}%)  [{t['exit_reason']}]"
        )


def main():
    parser = argparse.ArgumentParser(description="Бэктест торговой стратегии")
    parser.add_argument("--config", type=str, default="config/config.yaml")
    parser.add_argument("--db", type=str, default="data/backtest.db")
    parser.add_argument("--has_oi", type=bool, default=True)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if not Path(args.db).exists():
        # sqlite3 иначе молча создаст пустой файл по несуществующему пути, и
        # ошибка всплывёт позже непонятным "no such table: candles" вместо
        # очевидной "файла нет".
        print(f"Файл БД не найден: {args.db}")
        return

    result = run_backtest(args.config, args.db, args.has_oi)

    if not result:
        print("Нет данных")
        return

    # Загружаем реальные сделки из той же БД (если есть)
    live = _load_live_stats(args.db)

    # Сводка бэктеста
    print("\n" + "=" * 60)
    print("  РЕЗУЛЬТАТЫ БЭКТЕСТА")
    print("=" * 60)
    print(f"  OI проверка:        {'✅ Да' if result['has_oi'] else '❌ Нет'}")
    print(f"  MarketContext:      {'✅ Да' if result.get('has_mc', False) else '❌ Нет'}")
    print(f"  Период:            {result['period']}")
    print(f"  Сигналов:          {result['signals']}")
    print(f"  Сделок:            {result['trades']}")
    print(f"  Плюс / Минус:      {result['wins']} / {result['losses']}")
    print(f"  Win rate:          {result['win_rate']}%")
    print(f"  Total PnL:         ${result['total_pnl']:+.2f} (net of fees)")
    print(f"  Средний PnL:       ${result['avg_pnl']:+.2f}")
    print(f"  Комиссии всего:    ${result['total_fees']:.2f} (сред. ${result['avg_fee']:.4f}/сделку)")
    print(f"  TP: {result['tp_wins']} | SL: {result['sl_losses']} | Time: {result['time_exits']}")
    print(f"  Частичных закрытий: {result['partials']}")
    if result["pending_filled"] or result["pending_expired"]:
        total_pending = result["pending_filled"] + result["pending_expired"]
        fill_rate = result["pending_filled"] / total_pending * 100 if total_pending else 0
        print(
            f"  Pending-входы:     исполнено {result['pending_filled']} / "
            f"истекло {result['pending_expired']} ({fill_rate:.0f}% fill rate)"
        )
    if result["best"]:
        print(f"  Лучшая:  {result['best']['symbol']} ${result['best']['pnl']:+.2f}")
    if result["worst"]:
        print(f"  Худшая:  {result['worst']['symbol']} ${result['worst']['pnl']:+.2f}")
    print("=" * 60)

    # Сравнение с реальной торговлей
    if live:
        _print_comparison(result, live)

    _print_trade_list(result["trades_list"], "Сделки бэктеста")


if __name__ == "__main__":
    main()
