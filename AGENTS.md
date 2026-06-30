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
│   ├── utils.py               # Общие утилиты: timeframe_to_minutes, calculate_oi_slope_pct
│   ├── data_provider.py       # DataProvider — единый слой загрузки данных с in-memory кешем
│   ├── detector.py            # SetupDetector — основная стратегия (объём + OI + цена)
│   ├── market_context.py      # MarketContext — рыночный контекст (OTHERS Supertrend + BTC)
│   ├── price_surge.py         # PriceSurgeDetector — пампинг по чистой цене (только сигналы)
│   └── price_surge_service.py # PriceSurgeSignalProcessor — обогащение и отправка сигналов пампа
├── executor/
│   └── position_manager.py    # PositionManager — открытие/закрытие/трекинг позиций
├── notifier/
│   └── telegram_bot.py        # TelegramNotifier — бот с командами и отправкой сигналов
├── storage/
│   ├── database.py            # engine, async_session, init_db (с авто-ALTER TABLE)
│   ├── models.py              # ORM: Candle, Ticker, OpenInterest, Signal, Trade, PriceSurgeSignal, MarketContextSnapshot
│   └── stats.py               # trade_stats() — сбор статистики для команды /stats
├── backtest/
│   ├── loader.py              # Загрузка исторических данных в data/backtest.db
│   └── runner.py              # Симуляция стратегии на исторических свечах
├── scripts/
│   ├── backtest_sweep.py       # Подбор оптимальных параметров стратегии
│   ├── analyze_missed_signals.py # Поиск пропущенных сетапов (сильные движения без сигналов)
│   ├── analyze_performance.py  # Комплексный анализ на нескольких БД (свип + комбинации)
│   ├── test_blowoff_filter.py  # Тест фильтра памп-энд-дампов
│   └── test_improved_filters.py # Тест расширенных фильтров (breadth, extended price)
tests/
│   ├── test_data_provider.py     # Тесты DataProvider и CandleCache
│   ├── test_detector.py          # Тесты SetupDetector (volume pattern, price trend)
│   └── test_position_manager.py  # Тесты PositionManager (Circuit Breaker, TP/SL, позиции)
config/
├── config.yaml                # Боевая конфигурация
├── config.example.yaml        # Пример с комментариями
└── test-config.yaml           # Тестовая конфигурация
data/
└── trading_bot.db             # База SQLite (создаётся при первом запуске)
migrations/                    # Alembic-миграции
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
  ├── init_db()                           # Создание/обновление таблиц
  ├── ExchangeConnector × N (данные)       # По одному на биржу из config.exchanges
  ├── ExchangeConnector (торговля, real)   # С ключами API
  ├── SetupDetector                       # Основная стратегия
  ├── PriceSurgeDetector (опционально)     # Вторая стратегия (без торговли)
  ├── TelegramNotifier × 2 (опционально)   # Основной бот + бот PriceSurge
  ├── PositionManager (real)               # Управление позициями
  ├── MarketDataCollector                  # Бесконечный цикл: данные → аналитика → сигналы
  └── Application.wait()                   # Блокировка до SIGINT/SIGTERM
```

**Цикл сбора** (`MarketDataCollector._collect_cycle`, вызывается каждые `interval_seconds`):

1. `fetch_tickers()` со всех бирж → кросс-биржевой фильтр (монета должна быть на ByBit)
2. Фильтрация: USDT-пары, исключения, мин. объём (`max(bybit_vol, binance_vol)`)
3. Сохранение Ticker, свечей OHLCV, Open Interest в БД
4. `commit session` → вызов `_on_collect_cycle_done(session)`

**Обработка после цикла** (`Application._on_collect_cycle_done`):

0. Создаётся **общий `DataProvider`** на цикл — внедряется в оба детектора и `PriceSurgeSignalProcessor`
   - Кеширует свечи и OI в памяти → один запрос к БД на символ
0. **`MarketContext.update()`** (throttled: раз в 30 мин) — OTHERS из TradingView + BTC с биржи → режим
   - При смене режима → уведомление в Telegram
   - Режим передаётся в `PositionManager` (блок входа в risk-off, 50% размера в cautious)
   - Режим передаётся в `SetupDetector.apply_regime_multiplier()` (×1.5 к `volume_surge_mult` в cautious)
   - **Сохраняется снимок в `market_context_snapshots`** для использования в бэктестах
1. `PositionManager.update_positions()` — проверка TP/SL/времени
2. `SetupDetector.analyze()` → для каждого сигнала:
   - Сохранить Signal в БД
   - `PositionManager.open_position()` — попытка открыть позицию (с учётом рыночного режима)
   - Если позиция НЕ открыта → записать причину в `Signal.missed_reason`
   - `TelegramNotifier.send_signal()` — сигнал в Telegram с реальным статусом
3. `PriceSurgeSignalProcessor.process_and_notify()` → для каждого сигнала пампа:
   - Запросить цены/OI/часовой рост
   - Сохранить PriceSurgeSignal в БД
   - Отправить через отдельный Telegram-бот

## Рыночный контекст (`MarketContext`)

Оценивает глобальное состояние рынка и определяет режим торговли. Данные обновляются раз в 30 минут, в промежутках используется кешированный режим.

### Источники данных

| Индикатор | Источник | Таймфрейм | Что измеряет |
|-----------|----------|-----------|-------------|
| OTHERS index | TradingView (`CRYPTOCAP:OTHERS`) через `tvDatafeed` | 1h | Капитализация рынка без top-10 |
| Supertrend | Вычисляется на OTHERS | 1h (10, 3.0) | Тренд альт-рынка |
| BTC 1h change | Биржа (`fetch_ohlcv`) + тикеры из БД | 1h | Риск-режим (бегство в BTC или аппетит к риску) |

### Режимы торговли

| Режим | Условие | Вход в позиции | Размер позиции | Volume surge порог |
|-------|---------|---------------|----------------|-------------------|
| 🟢 RISK-ON | BTC > −1.5% **И** OTHERS Supertrend зелёный | ✅ Да | 100% | ×1.0 (15.0) |
| 🟡 CAUTIOUS | Один из сигналов негативный | ⚠️ Только при ST=green | **50%** | **×1.5 (22.5)** |
| 🔴 RISK-OFF | BTC падает >1.5% **И** OTHERS Supertrend красный | ❌ Нет | 0% | — |

**CAUTIOUS + ST=red** блокирует входы (аудит июня 2026: 5/5 убыточных сделок в этом режиме).

В CAUTIOUS режиме `volume_surge_mult` увеличивается на `cautious_volume_surge_mult_increase_pct`% (по умолчанию 50%) — бот берёт только самые сильные сетапы. Множитель применяется через `SetupDetector.apply_regime_multiplier()` каждый цикл.

### Сохранение в БД

Каждый цикл (после `MarketContext.update()`) текущее состояние сохраняется в таблицу `market_context_snapshots` через метод `MarketContext.get_snapshot()`. Это позволяет бэктестам загружать историю рыночного контекста и симулировать фильтрацию по режиму (risk_off → блок входа, cautious → повышенный volume_surge_mult). Если таблица отсутствует в БД — бэктест логирует предупреждение и продолжает без режимной фильтрации.

### Команда `/trend`

Возвращает: текущий режим с длительностью, Supertrend OTHERS, BTC 1h, OTHERS 1h, предыдущий режим.

### Конфигурация (`config.yaml → market_context`)

| Параметр | По умолчанию | Смысл |
|----------|-------------|-------|
| `enabled` | `true` | Включить/выключить |
| `btc_drop_threshold_pct` | 1.5 | Порог падения BTC для cautious/risk-off |
| `supertrend_atr_period` | 10 | Период ATR для Supertrend |
| `supertrend_multiplier` | 3.0 | Множитель ATR (ширина канала) |
| `altcoin_sample_size` | 30 | Запасной параметр (не используется с TradingView) |
| `notify_on_change` | `true` | Уведомлять о смене тренда |

## Логика стратегий

### Основная стратегия (`SetupDetector`) — объём + OI + цена

Алгоритм детекции (направление — только long):

1. **Выборка символов** — через `DataProvider.get_active_symbols()`: все монеты с тикером ByBit и свечами
2. **Проверка объёма** (`check_volume_pattern`, публичный метод):
   - Медиана объёма за `baseline_bars` свечей = норма
   - Если `min_baseline_volume_usdt > 0` → проверка медианы объёма × цена закрытия ≥ порог (фильтр низколиквидных)
   - Все `sustain_bars` последних свечей должны иметь объём ≥ `норма × volume_surge_mult`
   - Smoothness-фильтр: `max / median_recent ≤ smooth_max_ratio` (отсекает одиночные выбросы)
   - Dump-фильтр: объём последней свечи ≤ медиана остальных sustain-свечей × `dump_volume_mult`
3. **Проверка OI** (`_check_oi_trend`):
   - 3 последних записи OI → `calculate_oi_slope_pct()` из `utils.py`
   - Наклон в % от среднего OI ≥ `oi_slope_min_pct` (растущий открытый интерес = приток капитала)
4. **Проверка цены** (`check_price_trend`, публичный метод):
   - Рост за sustain-окно: `price_growth_min_pct ≤ рост`
   - **Exhaustion filter**: если рост > `exhaustion_gain_pct` И последняя свеча закрылась в верхних `exhaustion_pos_ratio` диапазона → сигнал блокируется (истощение покупателей)
   - **Страховочный потолок**: рост > `price_growth_max_pct` → блок (экстремальный памп)
   - Защита от рагпулов: падение за час ≤ `max_hourly_drop_pct`
5. **Уверенность** = `min(surge_multiple × 20, 95)`, где surge_multiple = средний_объём_окна / медиана_базового

**Ключевые параметры** (`config.yaml → strategy`):

| Параметр | Значение по умолчанию | Смысл |
|----------|----------------------|-------|
| `volume_surge_mult` | 15.0 | Во сколько раз объём превышает норму |
| `sustain_bars` | 4 | Сколько свечей подряд выше порога |
| `baseline_bars` | 70 | База для расчёта нормального объёма |
| `min_baseline_volume_usdt` | 3000 | Мин. медиана объёма в USDT (фильтр низкой ликвидности) |
| `oi_slope_min_pct` | 2.0% | Минимальный наклон OI |
| `price_growth_min_pct` | 1.0% | Мин. рост цены за sustain-окно |
| `price_growth_max_pct` | 12.0% | Страховочный потолок роста (0 = выкл) |
| `exhaustion_gain_pct` | 5.0% | Порог роста для exhaustion-фильтра |
| `exhaustion_pos_ratio` | 0.7 | Позиция закрытия свечи (0=low, 1=high) |
| `smooth_max_ratio` | 5.0 | Макс. отношение макс/медиана объёма |
| `dump_volume_mult` | 0.0 | Защита от свечей-выбросов (0 = выкл) |
| `max_hourly_drop_pct` | 10.0% | Защита от рагпулов |
| `cautious_volume_surge_mult_increase_pct` | 50.0% | На сколько % увеличить `volume_surge_mult` в CAUTIOUS режиме (0 = без изменений) |

### Вторая стратегия (`PriceSurgeDetector`) — чистый пампинг

Только информационная (нет торговли). Отдельный Telegram-бот.

Алгоритм: `change_pct = (close[-1] / open[0] - 1) × 100` за окно `price_surge_minutes` минут. Если `change_pct ≥ price_surge_pct` → сигнал.

Обогащение сигналов вынесено в `PriceSurgeSignalProcessor.process_and_notify()`:
- Точные цены открытия/закрытия за окно
- Рост за 1 час
- Изменение OI за 3 точки
- Количество сигналов по тикеру за сутки
- Ссылка на CoinGlass для визуализации

## Управление позициями (`PositionManager`)

### Открытие позиции
1. **Guard-проверки**: risk_off, circuit_breaker, лимит позиций, бан-лист, дубликат, кулдаун (`cooldown_hours`, по умолчанию 1ч), нет цены
2. **Бюджет риска**: `баланс × risk_per_trade_pct / 100 × position_size_mult × cb_mult` (с биржи), где `position_size_mult` — множитель рыночного режима, `cb_mult` — множитель Circuit Breaker
3. **TP/SL**: `sl = entry × (1 − stop_loss_pct/100)`, `tp = entry + (entry × stop_loss_pct/100) × risk_reward_ratio`
4. **Размер позиции**: `quantity = risk_budget / (entry × stop_loss_pct/100)`
5. **Real**: `set_leverage()` → `create_market_order(buy)` → ожидание 2с → запрос реальной цены → `set_tpsl()` (TP/SL на бирже) → `place_reduce_only_limit` (частичная фиксация)

### Мониторинг (каждый цикл)
1. Сверка позиций с биржей (закрытые по TP/SL → запись реальной цены выхода)
2. Проверка исполнения лимитника частичной фиксации → перевод SL в безубыток
3. **Partial close fallback**: если лимитник не был выставлен — проверка по тикеру, закрытие 50% + SL в безубыток
4. **Time exit**: превышение `max_hold_hours` → закрытие по рынку

### Финансовый учёт
- `pnl` — суммарный PnL по всей позиции (включая частичные закрытия)
- `partial_pnl` — PnL от частичного закрытия
- Комиссия пока не учитывается

### Конфигурация (`config.yaml → trading`)

| Параметр | По умолчанию | Смысл |
|----------|-------------|-------|
| `risk_per_trade_pct` | 1.0 | % от депозита, которым рискуем за один стоп |
| `risk_reward_ratio` | 3.0 | Соотношение TP/SL (3.0 = 1:3 risk/reward) |
| `stop_loss_pct` | 5.0 | Стоп-лосс, % от цены входа |
| `max_hold_hours` | 48.0 | Макс. время удержания позиции |
| `partial_close_enabled` | `true` | Частичная фиксация (всегда включена, игнорируется) |
| `partial_close_pct` | 50.0 | % пути до TP для частичного закрытия |
| `cooldown_hours` | 1.0 | Кулдаун после закрытия позиции (0 = без кулдауна) |
| `circuit_breaker_enabled` | `true` | Включить Circuit Breaker — защиту от серий убытков |
| `circuit_breaker_loss_streak_reduce` | 3 | После N убытков подряд уменьшить размер позиции |
| `circuit_breaker_reduce_mult_pct` | 50.0 | Множитель размера при срабатывании, % |
| `circuit_breaker_loss_streak_stop` | 5 | После N убытков подряд полностью остановить торговлю |
| `circuit_breaker_stop_minutes` | 60 | На сколько минут остановить торговлю |

### Circuit Breaker (защита от серий убытков)

Встроен в `PositionManager`. Отслеживает количество убыточных сделок подряд. При достижении порогов:

1. **3 убытка подряд** (`circuit_breaker_loss_streak_reduce`) → размер позиции уменьшается до `circuit_breaker_reduce_mult_pct`% (по умолчанию 50%)
2. **5 убытков подряд** (`circuit_breaker_loss_streak_stop`) → полная остановка торговли на `circuit_breaker_stop_minutes` минут (по умолчанию 60)
3. **Любая прибыльная сделка** → сброс счётчика, возобновление нормальной торговли

Статус `circuit_breaker_stop` возвращается `open_position()` и записывается в `signals.missed_reason`. Бэктест-раннер симулирует ту же логику.

### Расчёт позиции

- **Стоп-лосс**: `sl = entry × (1 − stop_loss_pct / 100)` — фиксированный % от цены входа
- **Тейк-профит**: `tp = entry + (entry × stop_loss_pct / 100) × risk_reward_ratio` (например +15% при SL=5%, ratio=3.0)
- **Размер позиции**: `quantity = risk_budget / sl_distance`, где `risk_budget = баланс × risk_per_trade_pct / 100`
  - Риск в долларах фиксирован относительно депозита
  - Множитель рыночного режима применяется к `risk_budget` (CAUTIOUS = ×0.5)

## База данных

SQLite в WAL-режиме (`data/trading_bot.db`). Миграции: Alembic + ручной `ALTER TABLE` в `init_db()` для обратной совместимости.

**Модели:**

| Таблица | Назначение |
|---------|-----------|
| `tickers` | Последний тикер (цена, объём) — exchange + symbol |
| `candles` | OHLCV-свечи — уникальность по exchange + symbol + timestamp |
| `open_interest` | OI — сохраняется только при изменении значения |
| `signals` | Сигналы основной стратегии, включает `missed_reason` (причина пропуска) |
| `price_surge_signals` | Сигналы PriceSurgeDetector |
| `trades` | Торговые позиции (вход/выход, PnL, partial close, TP/SL статус) |
| `market_context_snapshots` | Снимки рыночного контекста (regime, trend, Supertrend, BTC/OTHERS) |

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
- `cooldown` — кулдаун после закрытия (длительность: `cooldown_hours`)
- `risk_off` — входы заблокированы рыночным режимом
- `no_price` — нет цены для расчёта
- `error` — ошибка создания ордера / монета в чёрном списке
- `circuit_breaker_stop` — Circuit Breaker: полная остановка после серии убытков
- `disabled` — торговля выключена

Причина пропуска записывается в БД в поле `signals.missed_reason`, детали ошибки — в `signals.missed_detail`.

### Защита от каскада ошибок

Если по одному символу происходит 3 ошибки `open_position` подряд — символ получает кулдаун на 4 часа (`_error_cooldown_until`). Счётчик сбрасывается после первой успешной сделки. Защищает от ситуации CBRS × 4 ошибки за 30 минут (июнь 2026).

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
- **DataProvider кеширует на один цикл** — создаётся новый экземпляр в `_on_collect_cycle_done`, внедряется в оба детектора и processor. Кеш живёт до конца цикла, затем объект выбрасывается. Никакого TTL, никаких устаревших данных.
- **Двойной механизм миграций** — Alembic для структуры + `ALTER TABLE` в `init_db()` для добавления колонок. При больших изменениях схемы лучше использовать только Alembic.
- **Partial close в бэктесте** — срабатывает только если цена достигает halfway-уровня. Сделки, где цена сразу пошла к SL, не получают защиты от частичной фиксации.
- **MarketContext в бэктесте** — если в БД есть таблица `market_context_snapshots` (записи от live-бота), бэктест загружает их и применяет: risk_off блокирует входы, cautious повышает `volume_surge_mult`. Если таблицы нет — логируется предупреждение, бэктест продолжает без режимной фильтрации.

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

# Docker
make docker-build
make docker-up
make docker-logs
make docker-down

# Бэктест
make backtest-load        # Загрузка 7 дней истории
make backtest-run ARGS="--days 30"  # Прогон на истории
make backtest-run-live                # Бэктест на живой БД + сравнение с реальными сделками

# Поиск оптимальных параметров (подбор конфигурации)
.venv/bin/python scripts/backtest_sweep.py   # Прогон 37 конфигураций на trading_bot_*.db
                                             # Результаты: data/backtest_sweep_results.json
                                             # Лог: data/backtest_sweep_output.txt

# Миграции
make migrate-create name=add_column
make migrate-up

# Тесты
make test

# Анализ пропущенных сигналов
.venv/bin/python scripts/analyze_missed_signals.py    # Поиск монет с сильными движениями без сигналов
.venv/bin/python scripts/analyze_performance.py        # Комплексный бэктест-анализ (параметр-свип + комбинации)
.venv/bin/python scripts/test_blowoff_filter.py        # Тест фильтра "blow-off top" против памп-энд-дампов
.venv/bin/python scripts/test_improved_filters.py      # Тест расширенных фильтров (market breadth, extended price)
```

## Подбор параметров (`scripts/backtest_sweep.py`)

Скрипт для автоматического перебора ключевых параметров стратегии на исторических данных. Прогоняет заданный набор значений для каждого параметра, сохраняет результаты и определяет наилучшую конфигурацию.

**Что перебирается:**
- `risk_reward_ratio` — соотношение TP/SL (2.0, 2.5, 3.0, 3.5, 4.0, 5.0)
- `volume_surge_mult` — порог объёма (10, 12, 15, 18, 20, 25)
- `cooldown_hours` — кулдаун после закрытия (0, 0.5, 1, 2, 4, 8)
- `stop_loss_pct` — ширина стопа (3%, 4%, 5%, 6%, 7.5%, 10%)
- `sustain_bars` — длительность сустейна (3, 4, 5, 6)
- `exhaustion_gain_pct` — exhaustion-фильтр (0=выкл, 5%, 8%, 10%)
- `dump_volume_mult` — фильтр свечей-выбросов (0=выкл, 2, 3, 5, 8)
- `partial_close_pct` — % пути до TP для частичной фиксации (40%, 50%, 60%)

**Использование:**
```bash
# Положить БД в data/ и указать путь в скрипте (DB_PATH)
.venv/bin/python scripts/backtest_sweep.py
```
Занимает ~3 часа на БД размером ~1GB. Каждый параметр тестируется независимо, результаты — PnL, win rate, TP/SL/Time, partials.

**Важно:** скрипт НЕ тестирует комбинации параметров — каждый параметр перебирается при фиксированных остальных (базовый конфиг). Для проверки совместного эффекта лучших параметров нужно запустить отдельный прогон с комбинированными настройками.

## Архитектурные решения (не пересматривать без новых данных)

Зафиксированы по результатам аудита июня 2026.

### ATR-адаптивный SL — не применять

Стратегия торгует «накачки» (volume surges) на низколиквидных альткоинах. В момент пампа волатильность взрывается — исторический ATR не показателен. 3-минутные свечи MANTA имеют диапазон 5-10% на пампе, ATR(14) на спокойном рынке в разы меньше. ATR-адаптивный SL будет либо слишком узким (на истории), либо слишком широким (подстроившись под памп).

**Решение:** фиксированный SL с возможностью небольшого увеличения для волатильных монет. Обсуждалось: stop_loss_pct 7% как компромисс (вместо 5%). Не ATR.

### Partial close — лимитный ордер при открытии

При открытии позиции сразу выставляется reduce-only лимитный ордер на 50% объёма по цене частичной фиксации. Биржа исполняет его мгновенно при достижении цены — не зависит от цикла опроса бота. После исполнения SL переводится в безубыток для остатка позиции.

Если лимитник не удалось выставить — `update_positions()` проверяет частичную фиксацию по тикеру как fallback (с проверкой на отсутствие открытых ордеров после рестарта).

### Exhaustion filter — известная проблема (исправлено)

Фильтр `exhaustion_gain_pct` сравнивает `close[-1]` с `open[-sustain]`. При классическом pump-and-dump (POPCAT: +17.8% за 12 мин, TAIKO: +24.6% за 27 мин) памп и дамп происходят внутри sustain-окна. К моменту сигнала `change_pct` уже маленький или отрицательный, `close_pos` низкий → фильтр не срабатывает.

**Реализовано (июнь 2026):** добавлен **exhaustion filter v2** — проверка экстремального пампа от baseline:
- Вычисляется медиана close за `baseline_bars` (нормальный уровень цены)
- Вычисляется max high в sustain-окне
- Если рост от baseline до max high > `exhaustion_gain_pct × 6` (по умолчанию 30%) → блокировка
- Не зависит от `close_pos` — ловит случаи, где дамп уже начался и последняя свеча закрылась низко
- Порог 30% выбран по данным июня 2026: MANTA (+67%), TAIKO (+40.9%) блокируются; HEI (+28%) пропускается (прибыльная)

### CAUTIOUS + ST=red — блокировать входы

По данным июня 2026: 5/5 сделок в режиме CAUTIOUS + Supertrend=red убыточны. Бэктест на тех же свечах подтверждает — ни одна не дошла до TP. Реализовано в `should_block_entries()` (market_context.py:145-155).

## Ключевые точки расширения

- **Новая стратегия** — реализовать `BaseDetector.analyze()`, добавить детектор в `Application.start()`. Использовать `DataProvider` для загрузки данных (кеш на цикл).
- **Новый фильтр в детекторе** — добавить метод в `SetupDetector`, вызвать из `check_price_trend` или `check_volume_pattern`. Добавить параметры в `StrategyConfig` с дефолтом 0 (= выкл).
- **Новый сервис-обработчик** — по аналогии с `PriceSurgeSignalProcessor`: инкапсулирует обогащение сигналов, persistence и нотификации.
- **Новая биржа** — добавить `ExchangeConfig` в `config.yaml`, ccxt поддерживает её из коробки
- **Нотификации в другой канал** — реализовать аналог `TelegramNotifier` с тем же интерфейсом
