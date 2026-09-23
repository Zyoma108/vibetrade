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

from src.backtest.engine import DEFAULT_MARKETS_PATH, load_data, load_markets, simulate
from src.backtest.metrics import breakeven_credit, outcome_counts
from src.config import Settings

logger = logging.getLogger(__name__)


def run_backtest(config_path: str, db_path: str, has_oi: bool = True,
                 markets_path: str | None = DEFAULT_MARKETS_PATH) -> dict | None:
    """Загрузить БД и прогнать симуляцию. Тонкая обёртка над движком.

    `markets_path` — метаданные инструментов для модели шага лота. По
    умолчанию config/bybit_markets.json, если файл есть; None отключает
    округление и возвращает прежнее (до 22.09.2026) поведение.
    """
    settings = Settings.from_yaml(config_path)
    data = load_data(db_path)
    if not data["all_timestamps"]:
        return None
    return simulate(settings, data, has_oi=has_oi, markets=load_markets(markets_path))


def _load_live_stats(db_path: str, trading=None) -> dict | None:
    """Загрузить статистику реальных сделок из БД.

    `trading` — конфиг сделок: нужен только для веса безубытка в
    взвешенном winrate, без него колонка просто не появится.
    """
    import sqlite3

    try:
        db = sqlite3.connect(db_path)
    except Exception:
        return None

    # Таблицы может не быть вовсе: архив, выгруженный только со свечами и OI,
    # — законный вход для бэктеста, и падать на нём отчёт не должен.
    try:
        trades = db.execute(
            "SELECT symbol, direction, entry_price, exit_price, "
            "entry_time, exit_time, pnl, status, partial_closed, partial_pnl "
            "FROM trades WHERE status = 'closed' ORDER BY exit_time"
        ).fetchall()
    except sqlite3.OperationalError:
        db.close()
        return None

    if not trades:
        db.close()
        return None

    total_pnl = sum(t[6] or 0 for t in trades)
    # Классификация исхода — общая с движком (metrics.outcome): безубыток
    # отдельным классом, иначе снятая с б/у стопа сделка попадает в «плюс».
    # tp_price у боевых сделок не хранится — восстанавливается из конфига, как
    # его считает PositionManager._tp_price. Конфига нет — доля тейков не
    # появится в отчёте, а не посчитается неверно.
    tp_mult = (1 + trading.stop_loss_pct / 100 * trading.risk_reward_ratio) if trading else None
    counts = outcome_counts(
        [{"entry_price": t[2], "exit_price": t[3], "pnl": t[6] or 0.0, "direction": t[1],
          "tp_price": (t[2] * tp_mult if tp_mult and t[2] else None)}
         for t in trades],
        be_credit=(breakeven_credit(trading.risk_reward_ratio, trading.partial_close_pct,
                                    trading.partial_close_qty_pct) if trading else None),
    )

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
        **counts,
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
        ("Тейк / б/у / стоп",
         f"{bt['wins']} / {bt['breakevens']} / {bt['losses']}",
         f"{live['wins']} / {live['breakevens']} / {live['losses']}" if live else "—"),
        ("Доля ПОЛНЫХ ТЕЙКОВ (цель ≥25%)",
         f"{bt.get('full_take_rate', '—')}%",
         f"{live.get('full_take_rate', '—')}%" if live else "—"),
        ("Win rate (б/у не в счёт)",
         f"{bt['win_rate']}%",
         f"{live['win_rate']}%" if live else "—"),
        (f"Win rate (б/у по {1/bt['breakeven_credit']:.1f} за тейк)"
         if bt.get("breakeven_credit") else "Win rate взвешенный",
         f"{bt.get('win_rate_weighted', '—')}%",
         f"{live.get('win_rate_weighted', '—')}%" if live else "—"),
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
    parser.add_argument(
        "--markets", type=str, default=DEFAULT_MARKETS_PATH,
        help="метаданные инструментов для модели шага лота; пустая строка — выключить",
    )
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

    result = run_backtest(args.config, args.db, args.has_oi, args.markets or None)

    if not result:
        print("Нет данных")
        return

    # Загружаем реальные сделки из той же БД (если есть). Конфиг нужен для
    # веса безубытка — он берётся из ТЕКУЩЕГО конфига, поэтому на периоде, где
    # доля партиала менялась, взвешенный winrate приблизителен (см.
    # metrics.breakeven_credit).
    live = _load_live_stats(args.db, Settings.from_yaml(args.config).trading)

    # Сводка бэктеста
    print("\n" + "=" * 60)
    print("  РЕЗУЛЬТАТЫ БЭКТЕСТА")
    print("=" * 60)
    print(f"  OI проверка:        {'✅ Да' if result['has_oi'] else '❌ Нет'}")
    print(f"  MarketContext:      {'✅ Да' if result.get('has_mc', False) else '❌ Нет'}")
    # Шаг лота меняет размер позиции, поэтому прогоны с ним и без него не
    # сопоставимы напрямую — отмечаем явно.
    lot = result.get("amount_too_small")
    if args.markets and lot is not None:
        no_meta = result.get("symbols_without_lot_meta") or []
        note = f"✅ Да (отказов по мин. лоту: {lot}"
        if no_meta:
            note += f"; БЕЗ метаданных, дробный объём: {len(no_meta)} монет"
        print(f"  Шаг лота биржи:     {note})")
        if no_meta:
            print(f"      {', '.join(no_meta[:8])}"
                  + (f" и ещё {len(no_meta) - 8}" if len(no_meta) > 8 else ""))
    else:
        print("  Шаг лота биржи:     ❌ Нет — объём дробный")
    print(f"  Период:            {result['period']}")
    print(f"  Сигналов:          {result['signals']}")
    print(f"  Сделок:            {result['trades']}")
    print(f"  Тейк / б/у / стоп: {result['wins']} / {result['breakevens']} / {result['losses']}")
    if result.get("full_take_rate") is not None:
        rate = result["full_take_rate"]
        verdict = ("ЦЕЛЬ" if rate >= 25 else "порог" if rate >= 22
                   else "выше убыточности" if rate >= 18.9 else "УБЫТОЧНО")
        print(f"  Полных тейков:     {result['full_takes']} = {rate}% ({verdict}; "
              f"18.9% — безубыток системы, 22% — порог, 25% — цель 20%/мес при RR 2.0)")
    print(f"  Win rate:          {result['win_rate']}% (безубытки не в счёт, "
          f"их доля {result['breakeven_rate']}%)")
    if result.get("breakeven_credit"):
        print(f"  Win rate взвеш.:   {result['win_rate_weighted']}% "
              f"(безубыток = {result['breakeven_credit']:.3f} тейка, "
              f"{1/result['breakeven_credit']:.1f} безубытка на тейк; "
              f"зачтено {result['breakeven_equivalent_wins']} побед)")
    print(f"  Total PnL:         ${result['total_pnl']:+.2f} (net of fees)")
    print(f"  Средний PnL:       ${result['avg_pnl']:+.2f}")

    # --- Метрики решения ---------------------------------------------------
    # Winrate и Total PnL сами по себе решения не обосновывают: на выборках в
    # 40-110 сделок разница в пару долларов между конфигурациями укладывается
    # в ширину доверительного интервала. Сравнивать конфигурации следует по
    # E[R], и только когда интервал не накрывает ноль.
    if result["trades"]:
        ci = result.get("expectancy_R_ci")
        verdict = "ЗНАЧИМО" if result.get("expectancy_R_significant") else "не значимо"
        ci_text = f"[{ci[0]:+.3f}; {ci[1]:+.3f}] — {verdict}" if ci else "— (выборка мала)"
        print(f"  E[R] на сделку:    {result['expectancy_R']:+.3f}   95% ДИ {ci_text}")
        print(f"  Сумма R:           {result['total_R']:+.2f}"
              + (f"   ({result['R_per_day']:+.2f} R/сутки)" if result.get("R_per_day") else ""))
        if result.get("profit_factor"):
            print(f"  Profit factor:     {result['profit_factor']:.2f}")
        print(f"  Макс. просадка:    {result['max_drawdown_R']:.2f} R"
              f"   худшая серия убытков: {result['worst_loss_streak']}")
    if result.get("return_pct_per_30d") is not None:
        print(f"  Доходность:        {result['return_pct_of_deposit']:+.2f}% к депозиту"
              f"   ({result['return_pct_per_30d']:+.2f}% за 30 суток, без компаундинга)")
    if result.get("hold_hours_median") is not None:
        print(f"  Время в позиции:   медиана {result['hold_hours_median']:.1f} ч"
              f"   p90 {result['hold_hours_p90']:.1f} ч")
    if result.get("trades_per_day"):
        print(f"  Частота:           {result['trades_per_day']:.2f} сделок/сутки")
    print("-" * 60)
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
