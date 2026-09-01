# AGENTS.md — VibeTrade

Асинхронный торговый бот для криптобирж: детекция сетапов по объёму и открытому интересу,
управление позициями, Telegram-нотификации.

Этот файл — ядро: он грузится в каждую сессию целиком. Подробности вынесены в `docs/`,
читать их по триггерам из таблицы «Куда смотреть».

---

## ⚠️ Правила, которые нельзя нарушать

Каждое из них уже нарушалось и стоило либо испорченной БД, либо решений по стратегии,
принятых на неверных числах. Обоснования — по ссылкам, но сами правила действуют без чтения.

**1. К базе — только через `docker exec`, никогда напрямую с хоста.**
`data/` внутри контейнера — named Docker volume (`vibetrade_data`), не bind-mount. Хостовый
`sqlite3 data/trading_bot.db` физически не видит файл бота и открывает устаревшую копию.
Все обращения — `docker exec trading-bot <команда>` (`scripts/` скопированы в образ ради этого).
Хостовый `data/` содержит только архивные БД для бэктестов. Подробности: `docs/database.md`.

**2. Гейты стратегии в бэктесте не переписывать своим кодом.**
`src/backtest/engine.py` обязан **звать** проверки детектора (`SetupDetector.check_volume_pattern`,
`check_price_trend`, `analytics/utils.oi_trend_passes()`), а не повторять их логику. Копия
OI-гейта разъезжалась с боевой дважды: первый раз завысила свипы по RR/partial-close/retracement,
второй — срезала выборку с 49 сигналов до 8. Подробности: `docs/backtest.md`.

**3. Parity бэктеста проверяется только прогоном на ветке ДО изменений.**
Сравнить два прогона ПОСЛЕ правки — не доказательство: именно так регрессия 49→8 прошла мимо
проверки. Baseline снимается через `git worktree add /tmp/vt_old <ветка-до>`, сравнивается
посделочный список на одной и той же БД. Подробности: `docs/backtest.md`.

**4. Каданс скана — часть стратегии, а не настройка.**
Sustain-окно детектора = 4 бара × 3 мин = 12 минут. Цикл длиннее — бот физически не видит сетап
внутри его окна. Верхняя граница гарантируется `SCAN_PHASE_TIMEOUT_SEC`, не надеждой.
Подробности и разбор бюджета цикла: `docs/operations.md`.

**5. `collectors.scan_cycle_seconds` читается ТОЛЬКО бэктестом.**
Это не период работы бота (тот — `interval_seconds`), а шаг, на котором движок ищет сигналы.
Ставить его надо по фактической медиане из лога, иначе свип описывает бота, которого не
существует. Сейчас в `config.yaml` — 75 с (дефолт в коде — 105 с). Живой период работы бота
меняется другим параметром — `collectors.interval_seconds` (сейчас 45 с).

**6. Прод на Python 3.12, локальный `.venv` — на 3.14.**
Тесты и бэктесты гоняются на одной минорной, прод работает на другой. Пока не проявлялось.
Свести к одной: либо `python3.12 -m venv --clear .venv && .venv/bin/pip install -e ".[dev]"`,
либо поднять `FROM python:3.14-slim` в `Dockerfile` (второе — изменение прод-рантайма).

---

## Куда смотреть

| Что делаешь | Читай перед этим |
|---|---|
| Правишь детектор, фильтры, пороги `strategy` | `docs/strategy.md` |
| Правишь вход/выход, TP/SL, Circuit Breaker, пороги `trading` | `docs/positions.md` |
| Трогаешь `src/backtest/*` или любой `scripts/sweep_*.py` | `docs/backtest.md` (обязательно) |
| Работаешь со схемой БД, пишешь SQL, лезешь в данные | `docs/database.md` |
| Ускоряешь цикл, правишь сборщик, Telegram, деплой | `docs/operations.md` |
| Предлагаешь изменение стратегии | `docs/decisions.md` (проверить, не отвергнуто ли уже) |

История экспериментов и аудитов (даты, цифры, что и почему откачено) лежит в тех же файлах,
в разделах «История» — это живой журнал, а не архив: сверяться с ним до предложений.

---

## Стек

- **Python 3.12** в проде (`Dockerfile`, `pyproject.toml`), `asyncio` — см. правило 6
- **ccxt** — унифицированный доступ к биржам (синхронный, wrapped в `asyncio.to_thread`)
- **aiogram 3.x** — Telegram Bot API (long polling)
- **SQLAlchemy 2.0 + aiosqlite** — SQLite в WAL-режиме, named Docker volume
- **Pydantic 2.x** — валидация конфигурации (YAML + `${ENV_VAR}`)
- **Docker Compose** — деплой (один контейнер, `restart: unless-stopped`)

Миграций нет: схема живёт в `src/storage/models.py` и применяется `init_db()`
(`create_all()` + ручной список `ALTER TABLE`/`DROP INDEX`). Alembic удалён 26.08.2026 —
почему и как заводить заново, если понадобится: `docs/database.md`.

## Файловая структура

```
src/
├── main.py                    # CLI-вход: аргументы, настройка логов, запуск Application
├── config.py                  # Pydantic-модели конфигурации, загрузка из YAML
├── core/app.py                # Application — оркестратор (инициализация, главный цикл, shutdown)
├── connectors/exchange.py     # ExchangeConnector — обёртка над ccxt (данные + торговля)
├── collectors/market_data.py  # MarketDataCollector — периодический сбор тикеров/свечей/OI
├── analytics/
│   ├── base.py                # Signal (dataclass), BaseDetector (ABC)
│   ├── utils.py               # Общие утилиты, в т.ч. oi_trend_passes() — общий OI-гейт
│   ├── data_provider.py       # DataProvider — единый слой загрузки данных с кешем на цикл
│   ├── detector.py            # SetupDetector — основная стратегия (объём + OI + цена)
│   ├── market_context.py      # MarketContext — режим рынка (OTHERS Supertrend + BTC)
│   ├── price_surge.py         # PriceSurgeDetector — пампинг по чистой цене (только сигналы)
│   └── price_surge_service.py # PriceSurgeSignalProcessor — обогащение сигналов пампа
├── executor/
│   ├── position_manager.py    # PositionManager — открытие/закрытие/трекинг позиций
│   └── guards.py              # TradingGuards — Circuit Breaker, бан-лист, кулдаун + персист
├── notifier/telegram_bot.py   # TelegramNotifier — бот с командами и отправкой сигналов
├── storage/
│   ├── database.py            # engine, async_session, init_db
│   ├── models.py              # ORM: Candle, Ticker, OpenInterest, Signal, Trade, ...
│   └── stats.py               # trade_stats() — статистика для /stats
└── backtest/
    ├── engine.py              # ЕДИНСТВЕННАЯ реализация цикла симуляции (см. правило 2)
    └── runner.py              # Отчёт: прогон + сравнение с реальными сделками из той же БД
scripts/                       # Свипы параметров (sweep_*.py) и анализаторы — см. docs/backtest.md
tests/                         # Юнит-тесты + золотой тест движка и parity runner↔engine
config/config.yaml             # Боевая конфигурация (единственная — YAML + ${ENV_VAR} из .env)
```

## Режимы работы

| Режим | `trading.mode` | Торговля | Токены API |
|-------|---------------|----------|------------|
| `signal` | Только сбор данных и сигналы в Telegram | Нет | Не нужны |
| `real` | Реальная торговля на бирже | Через API ByBit | Обязательны |

## Архитектура — главный цикл

Запуск и работа управляются `Application` (`core/app.py`):

```
Application.start()
  ├── init_db()                            # Создание/обновление таблиц
  ├── ExchangeConnector × N (данные)       # По одному на биржу из config.exchanges
  ├── ExchangeConnector (торговля, real)   # С ключами API
  ├── SetupDetector                        # Основная стратегия
  ├── PriceSurgeDetector (опционально)     # Вторая стратегия (без торговли)
  ├── TelegramNotifier × 2 (опционально)   # Основной бот + бот PriceSurge
  ├── PositionManager (real)               # Управление позициями
  ├── MarketDataCollector                  # Бесконечный цикл: данные → аналитика → сигналы
  └── Application.wait()                   # Блокировка до SIGINT/SIGTERM
```

**Цикл сбора** (`MarketDataCollector._collect_cycle`, каждые `interval_seconds`):

1. `fetch_tickers()` со всех бирж → кросс-биржевой фильтр (монета должна быть на ByBit)
2. Фильтрация: USDT-пары, исключения, мин. объём (`max(bybit_vol, binance_vol)`)
3. Сохранение Ticker, свечей OHLCV, Open Interest в БД
4. `commit session` → вызов `_on_collect_cycle_done(session)`

**Обработка после цикла** (`Application._on_collect_cycle_done`) — одна транзакция на всё,
коммит в конце, сетевые вызовы к Telegram строго ПОСЛЕ коммита (`docs/operations.md`):

0. Общий `DataProvider` на цикл — внедряется в оба детектора и `PriceSurgeSignalProcessor`
0. `MarketContext.update()` (раз в 30 мин) → режим рынка → множители размера и порогов
1. `PositionManager.update_positions()` — проверка TP/SL/времени/pending-входов
2. `SetupDetector.analyze()` → Signal в БД → `open_position()` → статус в Telegram
3. `PriceSurgeSignalProcessor.process()` → сигналы пампа во второй бот

## Конфигурация и запуск

- **Конфиг**: `config/config.yaml` (YAML + подстановка `${ENV_VAR}` из `.env`) — единственный
- **Секреты**: `.env` (не коммитится) — токены API и Telegram
- **Две стратегии**: `strategy` (с торговлей) и `strategy_price_surge` (только сигналы)
- **Два Telegram-бота**: `telegram` и `telegram_price_surge` — токены обязаны быть разными

```bash
make run                     # Локально с config/config.yaml
make run-signal              # Режим "только сигналы"
make test                    # .venv/bin/pytest -v
make docker-build / docker-up / docker-logs / docker-down / docker-rebuild
make clean                   # __pycache__ и .pytest_cache

# Бэктест. runner принимает только --config / --db / --has_oi (никакого --days)
make backtest-run ARGS="--db data/trading_bot_2026-08.db"
make backtest-run-live       # docker cp живой БД в снапшот + прогон + сравнение со сделками

.venv/bin/python scripts/analyze_missed_signals.py   # Монеты с движениями без сигналов
.venv/bin/python scripts/sweep_rr.py                 # Свипы параметров — см. docs/backtest.md
```

## Ключевые точки расширения

- **Новая стратегия** — реализовать `BaseDetector.analyze()`, добавить детектор в
  `Application.start()`. Данные брать через `DataProvider` (кеш на цикл).
- **Новый фильтр в детекторе** — метод в `SetupDetector`, вызов из `check_price_trend` или
  `check_volume_pattern`; параметр в `StrategyConfig` с дефолтом 0 (= выкл). Если фильтр
  отсекает near-miss (сетап уже прошёл порог объёма) — заполнить `context["stage"]`/
  `context["reason"]` перед `return`, чтобы отказ попал в `filtered_signals`.
- **Новый сервис-обработчик** — по аналогии с `PriceSurgeSignalProcessor`: обогащение
  сигналов, persistence и нотификации в одном месте.
- **Новая биржа** — добавить `ExchangeConfig` в `config.yaml`, ccxt поддерживает из коробки.
- **Нотификации в другой канал** — аналог `TelegramNotifier` с тем же интерфейсом.
