# AGENTS.md — VibeTrade

Асинхронный торговый бот для криптобирж с детекцией сетапов по объёму и открытому интересу, управлением позициями и Telegram-нотификациями.

## Стек

- **Python 3.12**, `asyncio`
- **ccxt** — унифицированный доступ к биржам (синхронный, wrapped в `asyncio.to_thread`)
- **aiogram 3.x** — Telegram Bot API (long polling)
- **SQLAlchemy 2.0 + aiosqlite** — SQLite в WAL-режиме
- **Pydantic 2.x** — валидация конфигурации (YAML + `${ENV_VAR}`)
- **Alembic** — миграции схемы БД
- **Docker Compose** — деплой (один контейнер, `restart: unless-stopped`)

## Файловая структура

```
src/
├── main.py                    # CLI-вход: аргументы, настройка логов, запуск Application
├── config.py                  # Pydantic-модели конфигурации, загрузка из YAML
├── core/
│   └── app.py                 # Application — оркестратор (инициализация, главный цикл, shutdown)
├── connectors/
│   └── exchange.py            # ExchangeConnector — обёртка над ccxt (данные + торговля)
├── collectors/
│   └── market_data.py         # MarketDataCollector — периодический сбор тикеров/свечей/OI
├── analytics/
│   ├── base.py                # Signal (dataclass), BaseDetector (ABC)
│   ├── detector.py            # SetupDetector — основная стратегия (объём + OI + цена)
│   └── price_surge.py         # PriceSurgeDetector — пампинг по чистой цене (только сигналы)
├── executor/
│   └── position_manager.py    # PositionManager — открытие/закрытие/трекинг позиций
├── notifier/
│   └── telegram_bot.py        # TelegramNotifier — бот с командами и отправкой сигналов
├── storage/
│   ├── database.py            # engine, async_session, init_db (с авто-ALTER TABLE)
│   ├── models.py              # ORM-модели: Candle, Ticker, OpenInterest, Signal, Trade, PriceSurgeSignal
│   └── stats.py               # trade_stats() — сбор статистики для команды /stats
├── backtest/
│   ├── loader.py              # Загрузка исторических данных в data/backtest.db
│   └── runner.py              # Симуляция стратегии на исторических свечах
config/
├── config.yaml                # Боевая конфигурация
├── config.example.yaml        # Пример с комментариями
└── test-config.yaml           # Тестовая конфигурация
data/
└── trading_bot.db             # База SQLite (создаётся при первом запуске)
migrations/                    # Alembic-миграции
```

## Три режима работы

| Режим | `trading.mode` | Торговля | Токены API |
|-------|---------------|----------|------------|
| `signal` | Только сбор данных и сигналы в Telegram | Нет | Не нужны |
| `virtual` | Виртуальная торговля (бумажный счёт) | Симулируется в БД | Не нужны |
| `real` | Реальная торговля на бирже | Через API ByBit | Обязательны |

## Архитектура — главный цикл

Запуск и работа управляются `Application` (`core/app.py`):

```
Application.start()
  ├── init_db()                           # Создание/обновление таблиц
  ├── ExchangeConnector × N (данные)       # По одному на биржу из config.exchanges
  ├── ExchangeConnector (торговля, real)   # С ключами API
  ├── SetupDetector                       # Основная стратегия
  ├── PriceSurgeDetector (опционально)     # Вторая стратегия (без торговли)
  ├── TelegramNotifier × 2 (опционально)   # Основной бот + бот PriceSurge
  ├── PositionManager (virtual/real)       # Управление позициями
  ├── MarketDataCollector                  # Бесконечный цикл: данные → аналитика → сигналы
  └── Application.wait()                   # Блокировка до SIGINT/SIGTERM
```

**Цикл сбора** (`MarketDataCollector._collect_cycle`, вызывается каждые `interval_seconds`):

1. `fetch_tickers()` со всех бирж → кросс-биржевой фильтр (монета должна быть на ByBit)
2. Фильтрация: USDT-пары, исключения, мин. объём (`max(bybit_vol, binance_vol)`)
3. Сохранение Ticker, свечей OHLCV, Open Interest в БД
4. `commit session` → вызов `_on_collect_cycle_done(session)`

**Обработка после цикла** (`Application._on_collect_cycle_done`):

1. `PositionManager.update_positions()` — проверка TP/SL/времени
2. `SetupDetector.analyze()` → для каждого сигнала:
   - Сохранить Signal в БД
   - `PositionManager.open_position()` — попытка открыть позицию
   - `TelegramNotifier.send_signal()` — сигнал в Telegram с реальным статусом
3. `PriceSurgeDetector.analyze()` → для каждого сигнала:
   - Запросить цены/OI/часовой рост
   - Сохранить PriceSurgeSignal в БД
   - Отправить через отдельный Telegram-бот

## Логика стратегий

### Основная стратегия (`SetupDetector`) — объём + OI + цена

Алгоритм детекции (направление — только long):

1. **Выборка символов** — все монеты, у которых есть свечи и тикер ByBit
2. **Проверка объёма** (`_check_volume_pattern`):
   - Медиана объёма за `baseline_bars` свечей = норма
   - Если `min_baseline_volume_usdt > 0` → проверка медианы объёма × цена закрытия ≥ порог (фильтр низколиквидных)
   - Все `sustain_bars` последних свечей должны иметь объём ≥ `норма × volume_surge_mult`
   - Smoothness-фильтр: `max / median_recent ≤ 5.0` (отсекает одиночные выбросы)
3. **Проверка OI** (`_check_oi_trend`):
   - 3 последних записи OI → линейная регрессия через `np.polyfit`
   - Наклон в % от среднего OI ≥ `oi_slope_min_pct` (растущий открытый интерес = приток капитала)
4. **Проверка цены** (`_check_price_trend`):
   - Рост за sustain-окно: `price_growth_min_pct ≤ рост ≤ price_growth_max_pct`
   - Защита от рагпулов: падение за час ≤ `max_hourly_drop_pct`
5. **Уверенность** = `min(surge_multiple × 20, 95)`, где surge_multiple = текущая_цена / первая_цена_окна

**Ключевые параметры** (`config.yaml → strategy`):

| Параметр | Значение по умолчанию | Смысл |
|----------|----------------------|-------|
| `volume_surge_mult` | 15.0 | Во сколько раз объём превышает норму |
| `sustain_bars` | 4 | Сколько свечей подряд выше порога |
| `baseline_bars` | 70 | База для расчёта нормального объёма |
| `oi_slope_min_pct` | 1.0% | Минимальный наклон OI |
| `price_growth_min_pct` | 1.0% | Мин. рост цены за sustain-окно |
| `price_growth_max_pct` | 20.0% | Макс. рост (фильтр «уже поздно») |
| `max_hourly_drop_pct` | 10.0% | Защита от рагпулов |

### Вторая стратегия (`PriceSurgeDetector`) — чистый пампинг

Только информационная (нет торговли). Отдельный Telegram-бот.

Алгоритм: `change_pct = (close[-1] / open[0] - 1) × 100` за окно `price_surge_minutes` минут. Если `change_pct ≥ price_surge_pct` → сигнал.

В `Application._on_collect_cycle_done` для каждого сигнала дополнительно вычисляется:
- Точные цены открытия/закрытия за окно
- Рост за 1 час
- Изменение OI за 3 точки
- Количество сигналов по тикеру за сутки
- Ссылка на CoinGlass для визуализации

## Управление позициями (`PositionManager`)

### Открытие позиции
1. **Guard-проверки**: лимит позиций, бан-лист, дубликат, кулдаун (24ч), нет цены
2. **Размер позиции**: `position_size_pct`% от депозита ИЛИ фиксированный `position_size_usdt`
3. **Real**: `set_leverage()` → `create_market_order(buy)` → ожидание 2с → запрос реальной цены → `set_tpsl()` (TP/SL на бирже)
4. **Virtual**: запись в БД с расчётными уровнями TP/SL, отслеживание по цене

### Мониторинг (каждый цикл)
1. **Real**: сверка позиций с биржей (закрытые по TP/SL → запись реальной цены выхода)
2. **Breakeven at halfway**: при достижении `partial_close_pct`% пути до TP → перенос SL в безубыток
3. **Partial close**: при halfway → закрытие 50% позиции + SL в безубыток для остатка
4. **Time exit**: превышение `max_hold_hours` → закрытие по рынку
5. **Virtual TP/SL**: сравнение текущей цены с расчётными уровнями

### Финансовый учёт
- `total_pnl` — суммарный PnL по всей позиции
- `partial_pnl` — PnL от частичного закрытия
- Комиссия пока не учитывается

## База данных

SQLite в WAL-режиме (`data/trading_bot.db`). Миграции: Alembic + ручной `ALTER TABLE` в `init_db()` для обратной совместимости.

**Модели:**

| Таблица | Назначение |
|---------|-----------|
| `tickers` | Последний тикер (цена, объём) — exchange + symbol |
| `candles` | OHLCV-свечи — уникальность по exchange + symbol + timestamp |
| `open_interest` | OI — сохраняется только при изменении значения |
| `signals` | Сигналы основной стратегии |
| `price_surge_signals` | Сигналы PriceSurgeDetector |
| `trades` | Торговые позиции (вход/выход, PnL, partial close, TP/SL статус) |

## Telegram-боты

Два независимых бота (основной + price surge), каждый со своим токеном. **Важно:** токены должны быть разными, иначе `TelegramConflictError`.

### Команды основного бота

| Команда | Доступ | Описание |
|---------|--------|----------|
| `/start` | Все | Показать chat ID |
| `/status` | Авторизованные | Аптайм, сигналов отправлено, статус паузы |
| `/pause` | Авторизованные | Приостановить отправку сигналов |
| `/resume` | Авторизованные | Возобновить отправку сигналов |
| `/stats [day\|week\|month\|all]` | Авторизованные | Статистика торговли |
| `/positions` | Авторизованные | Открытые позиции с PnL |

Второй бот (price surge) — только отправка, без команд.

### Статусы сигналов (основная стратегия)
- `opened` — позиция открыта
- `limit` — нет свободных слотов
- `duplicate` — уже есть позиция по монете
- `cooldown` — кулдаун 24ч после закрытия
- `no_price` — нет цены для расчёта
- `error` — ошибка создания ордера
- `disabled` — торговля выключена

## Отказоустойчивость

- **Сеть**: ExchangeConnector — 3 ретрая с экспоненциальной задержкой (5/10/15с)
- **Telegram**: notify_all — 3 попытки на чат; start — до 5 ретраев (5/10/15/20с), `drop_pending_updates=True`
- **Биржа**: ошибки по отдельным символам логируются, не прерывают цикл
- **Старт**: сбой синхронизации позиций не останавливает бота
- **Деплой**: Docker `restart: unless-stopped`, логи с ротацией (10MB × 3)

## Нюансы и подводные камни

- **ccxt синхронный** — каждый вызов завёрнут в `asyncio.to_thread()` с семафором (5 одновременных). Это не идеально для высоких нагрузок, но достаточно при `interval_seconds ≥ 30`.
- **Кросс-биржевой фильтр** — монета должна присутствовать на ByBit (торговая биржа). Объём берётся как `max(bybit, binance)`, что помогает находить более широкие движения.
- **Чёрный список символов** — если ByBit возвращает «sign the required agreement», символ добавляется в `_banned_symbols` до перезапуска.
- **Порядок данных в детекторе** — свечи загружаются из БД в хронологическом порядке. Детектор ожидает, что они упорядочены по времени.
- **OI сохраняется только при изменении** — это экономит место, но означает что `_check_oi_trend` работает с тремя точками изменения, а не с тремя последовательными свечами.
- **Backtest привязан к приватным методам детектора** — `runner.py` вызывает `_check_volume_pattern` и `_check_price_trend` напрямую, а не через публичный `analyze()`.
- **Двойной механизм миграций** — Alembic для структуры + `ALTER TABLE` в `init_db()` для добавления колонок. При больших изменениях схемы лучше использовать только Alembic.

## Конфигурация и секреты

- **Конфиг**: `config/config.yaml` (YAML + подстановка `${ENV_VAR}` из `.env`)
- **Секреты**: `.env` (не коммитится), содержит токены API и Telegram
- **Две стратегии**: `strategy` (основная, с торговлей) и `strategy_price_surge` (только сигналы)
- **Два Telegram-бота**: `telegram` и `telegram_price_surge` (независимые токены)

## Запуск

```bash
# Локально
make run                  # Запуск с config/config.yaml
make run-signal           # Режим "только сигналы"
make run-virtual          # Виртуальная торговля

# Docker
make docker-build
make docker-up
make docker-logs
make docker-down

# Бэктест
make backtest-load        # Загрузка 7 дней истории
make backtest-run ARGS="--days 30"  # Прогон на истории

# Миграции
make migrate-create name=add_column
make migrate-up

# Тесты
make test
```

## Ключевые точки расширения

- **Новая стратегия** — реализовать `BaseDetector.analyze()`, добавить детектор в `Application.start()`
- **Новая биржа** — добавить `ExchangeConfig` в `config.yaml`, ccxt поддерживает её из коробки
- **Новый тип сигнала** — расширить `Signal.dataclass`
- **Нотификации в другой канал** — реализовать аналог `TelegramNotifier` с тем же интерфейсом
