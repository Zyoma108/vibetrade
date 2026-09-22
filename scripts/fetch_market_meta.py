"""Выгрузка шага лота и минимального объёма инструментов биржи.

Зачем. Бэктест считает объём позиции дробным числом (`qty = risk / sl_distance`),
а боевой путь округляет его ВНИЗ до шага лота и отказывается от сделки, если
после округления остаётся ноль (`PositionManager._place_market_entry` →
`ExchangeConnector.amount_to_precision`). На маленьком депозите это заметно:
замер 22.09.2026 при нотионале $11 (депозит ~$55, риск 1%, SL 5%) — средняя
потеря размера 3.2%, медиана 0.55%, p90 8.4%, и 6 монет из 778 недоступны
вовсе. При нотионале $200 те же величины падают до 0.22% и нуля.

Величина небольшая и НЕ объясняет разрыв бэктеста с реалом, но это системати-
ческая односторонняя поправка (округление всегда вниз), и держать её в модели
дешевле, чем каждый раз вспоминать про неё.

Файл кладётся в config/ (data/ в .gitignore, туда он не доехал бы ни до
репозитория, ни до Docker-образа) и читается движком, если присутствует. Обновлять при
заметном изменении состава инструментов; для архивных прогонов помнить, что
шаг лота на бирже мог с тех пор поменяться.

Использование:
    .venv/bin/python scripts/fetch_market_meta.py --out config/bybit_markets.json
"""

import argparse
import json
import sys
from pathlib import Path

import ccxt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def fetch(exchange_id: str = "bybit") -> dict:
    ex = getattr(ccxt, exchange_id)({"options": {"defaultType": "linear"}})
    markets = ex.load_markets()
    out = {}
    for symbol, m in markets.items():
        if not (m.get("swap") and m.get("linear") and m.get("quote") == "USDT"):
            continue
        step = m.get("precision", {}).get("amount")
        min_amount = (m.get("limits", {}).get("amount") or {}).get("min")
        if not step:
            continue
        out[symbol] = {"step": float(step),
                       "min_amount": float(min_amount) if min_amount else 0.0}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exchange", default="bybit")
    ap.add_argument("--out", default="config/bybit_markets.json")
    args = ap.parse_args()

    meta = fetch(args.exchange)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(meta, f, indent=1, sort_keys=True)
    print(f"{args.exchange}: {len(meta)} инструментов → {args.out}")


if __name__ == "__main__":
    main()
