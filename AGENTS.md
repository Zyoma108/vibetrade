# AGENTS.md — VibeTrade

Асинхронный торговый бот для криптобирж с детекцией сетапов по объёму и открытому интересу, управлением позициями и Telegram-нотификациями.

## Стек

- **Python 3.12**, `asyncio`
- **ccxt** — унифицированный доступ к биржам (синхронный, wrapped в `asyncio.to_thread`)
- **aiogram 3.x** — Telegram Bot API (long polling)
- **SQLAlchemy 2.0 + aiosqlite** — SQLite в WAL-режиме, named Docker volume (см. "База данных")
- **Pydantic 2.x** — валидация конфигурации (YAML + `${ENV_VAR}`)
- **Alembic** — миграции схемы БД
- **Claude Code** (подписка, не API-ключ) — ИИ-режим, опциональный оркестратор-скилл + сабагенты поверх алгоритма (см. ниже)
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
│   ├── position_manager.py    # PositionManager — открытие/закрытие/трекинг позиций (общая механика, algo+agent)
│   └── agent_position_manager.py # AgentPositionManager(PositionManager) — apply_agent_* (решаемое агентом поведение)
├── agent/                     # ИИ-режим (доп. режим, отдельный аккаунт) — см. раздел ниже
│   └── tools.py                # AgentToolkit (данные) + build_strategy_briefing() — вызывается из scripts/agent_*.py, не из Python-цикла бота
├── notifier/
│   └── telegram_bot.py        # TelegramNotifier — бот с командами и отправкой сигналов
├── storage/
│   ├── database.py            # engine, async_session, init_db (с авто-ALTER TABLE)
│   ├── models.py              # ORM: Candle, Ticker, OpenInterest, Signal, Trade, PriceSurgeSignal, MarketContextSnapshot, AgentDecision
│   └── stats.py               # trade_stats() — сбор статистики для команды /stats
├── backtest/
│   └── runner.py              # Симуляция стратегии на исторических свечах
├── scripts/
│   ├── backtest_sweep.py       # Подбор оптимальных параметров стратегии
│   ├── sweep_focused.py         # Фокусированный свип (RR×SL, vol, dump, risk, partial)
│   ├── analyze_missed_signals.py # Поиск пропущенных сетапов (сильные движения без сигналов)
│   ├── analyze_performance.py  # Комплексный анализ на нескольких БД (свип + комбинации)
│   ├── test_blowoff_filter.py  # Тест фильтра памп-энд-дампов
│   ├── test_improved_filters.py # Тест расширенных фильтров (breadth, extended price)
│   ├── agent_data.py            # ИИ-режим: CLI для сабагентов (только чтение, AgentToolkit)
│   ├── agent_briefing.py        # ИИ-режим: печатает strategy briefing из живого конфига
│   └── agent_actions.py         # ИИ-режим: CLI для оркестратора (открыть/подтянуть SL/продлить/закрыть)
tests/
│   ├── test_data_provider.py     # Тесты DataProvider и CandleCache
│   ├── test_detector.py          # Тесты SetupDetector (volume pattern, price trend)
│   ├── test_position_manager.py  # Тесты PositionManager (Circuit Breaker, TP/SL, позиции)
│   └── test_agent.py             # Тесты ИИ-режима (tighten SL, hold extension, source scoping, AgentToolkit, strategy briefing)
config/
└── config.yaml                # Боевая конфигурация (единственная — YAML + `${ENV_VAR}` из `.env`)
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
| 🟢 RISK-ON | BTC > −1.5% **И** OTHERS Supertrend зелёный | ✅ Да | 100% | ×1.0 (5.0) |
| 🟡 CAUTIOUS | Один из сигналов негативный | ⚠️ Только при ST=green | **50%** | **×1.5 (7.5)** |
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
   - **Pre-sustain pump filter**: если рост за 10 свечей (30 мин) ДО sustain-окна > `pre_surge_max_pct` → блок (монета уже улетела до сигнала)
   - **Exhaustion filter**: если рост > `exhaustion_gain_pct` И последняя свеча закрылась в верхних `exhaustion_pos_ratio` диапазона → сигнал блокируется (истощение покупателей)
   - **Страховочный потолок**: рост > `price_growth_max_pct` → блок (экстремальный памп внутри sustain-окна)
   - Защита от рагпулов: падение за час ≤ `max_hourly_drop_pct`
5. **Уверенность** = `min(surge_multiple × 5, 100)`, где surge_multiple = средний_объём_окна / медиана_базового

Шаги 3-4 (near-miss после прохождения порога объёма) логируются в таблицу `filtered_signals`
с указанием монеты и причины — см. "Аудит отфильтрованных сетапов" ниже.

**Ключевые параметры** (`config.yaml → strategy`):

| Параметр | Значение по умолчанию | Смысл |
|----------|----------------------|-------|
| `volume_surge_mult` | 5.0 | Во сколько раз объём превышает норму |
| `sustain_bars` | 4 | Сколько свечей подряд выше порога |
| `baseline_bars` | 70 | База для расчёта нормального объёма |
| `min_baseline_volume_usdt` | 5000 | Мин. медиана объёма в USDT (фильтр низкой ликвидности) |
| `oi_slope_min_pct` | 2.0% | Минимальный наклон OI |
| `price_growth_min_pct` | 1.0% | Мин. рост цены за sustain-окно |
| `price_growth_max_pct` | 12.0% | Страховочный потолок роста в sustain-окне (0 = выкл) |
| `pre_surge_max_pct` | 8.0% | Макс. рост за 30 мин ДО sustain-окна (0 = выкл) |
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
5. **Real (`pending_entry_pullback_pct == 0`)**: `set_leverage()` → `create_market_order(buy)` → ожидание 2с → запрос реальной цены → `set_tpsl()` (TP/SL на бирже) → `place_reduce_only_limit` (частичная фиксация)

### Pending-вход на откате (`pending_entry_pullback_pct`)

Решает проблему покупки на пике пампа: детектор по конструкции подтверждает сетап только после `sustain_bars` свечей уже растущего объёма — вход market-ордером в этот момент часто оказывается локальным пиком движения (см. аудит db-analyst: 46-69% убыточных сделок показывали просадку ≥2-3% в первые 15 мин после входа, независимо от фильтров).

Вместо немедленного market-ордера (`_place_market_entry`) при `pending_entry_pullback_pct > 0` выставляется **лимитный buy-ордер** на уровне `сигнальная_цена × (1 − pullback_pct/100)` (`_place_pending_entry`), с TP/SL/quantity, посчитанными сразу от известной цены лимита. `Trade.status = "pending"`, `pending_expires_at = now + pending_entry_timeout_minutes`. Pending-заявки занимают "слот" наравне с открытыми позициями (`_count_open`/`_has_position` учитывают оба статуса).

Каждый цикл `check_pending_entries()`:
- Если по символу появилась позиция на бирже → лимитник исполнился (как **maker**) → `_activate_pending_entry()`: перевод в `status="open"`, выставление TP/SL и лимитника частичной фиксации (то же самое, что раньше делалось сразу в `open_position`).
- Если `now >= pending_expires_at` и позиции всё ещё нет → `_expire_pending_entry()`: отмена ордера, `status="expired"` (не считается ни открытой, ни закрытой сделкой).

На agent-пайплайне (`source='agent'`) пока лимитник ждёт исполнения, `reeval-agent` может его
подвинуть, перевести в market или отменить по собственному решению (`status="cancelled"` —
отдельно от механического `expired`) — см. раздел "ИИ-режим" → "Сопровождение".

Бэктест (`runner.py`) симулирует то же самое: `PendingEntry` создаётся вместо `SimPosition` при сигнале, на каждом баре проверяется `low <= limit_price` (заполнение) или истечение таймаута — независимо от `CYCLE_DELAY_BARS`, как и проверка TP/SL уже открытых позиций.

**Свип по `pending_entry_pullback_pct` (июль 2026, 3 БД, см. память `pending-entry-pullback-sweep-july-2026`)**: немонотонная зависимость — мелкий откат (0.3-0.8%) хуже baseline на всех базах, глубокий (≥1.2%) лучше на 2 из 3 (кроме 22.06-30.06, где лучшие движения не дают отката вообще). Выбрано `1.5%` как лучшее по сумме PnL и PnL/сделку в свипе, но подтверждено не на всех периодах — требует дальнейшей валидации на новых данных.

### Мониторинг (каждый цикл)
1. Сверка позиций с биржей (закрытые по TP/SL → запись реальной цены выхода)
2. Проверка исполнения лимитника частичной фиксации → перевод SL в безубыток
3. **Partial close fallback**: если лимитник не был выставлен — проверка по тикеру, закрытие 50% + SL в безубыток
4. **Time exit**: превышение `max_hold_hours` → закрытие по рынку
5. **Pending-входы**: `check_pending_entries()` — исполнение или истечение таймаута (см. выше)

### Финансовый учёт
- `pnl` — суммарный PnL по всей позиции, **net of fees** (включая частичные закрытия, минус комиссия всех "ног" сделки)
- `partial_pnl` — PnL от частичного закрытия (gross, без вычета комиссии — комиссия учтена отдельно в `fee`)
- `fee` — суммарная комиссия по сделке: вход (taker) + резервный лимитник частичной фиксации, если исполнился (maker) + финальный выход (taker — TP/SL-триггер, time-exit и аварийное закрытие всегда исполняются как market). Ставки — `taker_fee_pct`/`maker_fee_pct` в конфиге (по умолчанию Bybit VIP0: 0.055%/0.02%)
- Позиции, восстановленные через `sync_positions()` после рестарта бота, получают `fee=0.0` — комиссия за уже прошедшие вне видимости бота "ноги" неизвестна

### Конфигурация (`config.yaml → trading`)

| Параметр | По умолчанию | Смысл |
|----------|-------------|-------|
| `risk_per_trade_pct` | 1.0 | % от депозита, которым рискуем за один стоп |
| `risk_reward_ratio` | 2.0 | Соотношение TP/SL (2.0 = TP на +10% при SL=5%) |
| `stop_loss_pct` | 5.0 | Стоп-лосс, % от цены входа |
| `max_hold_hours` | 48.0 | Макс. время удержания позиции |
| `partial_close_pct` | 35.0 | % пути до TP для частичного закрытия |
| `cooldown_hours` | 1.0 | Кулдаун после закрытия позиции (0 = без кулдауна) |
| `circuit_breaker_enabled` | `true` | Включить Circuit Breaker — защиту от серий убытков |
| `circuit_breaker_loss_streak_reduce` | 2 | После N убытков подряд уменьшить размер позиции |
| `circuit_breaker_reduce_mult_pct` | 50.0 | Множитель размера при срабатывании, % |
| `circuit_breaker_loss_streak_stop` | 3 | После N убытков подряд полностью остановить торговлю |
| `circuit_breaker_stop_minutes` | 60 | На сколько минут остановить торговлю |
| `taker_fee_pct` | 0.055 | Комиссия тейкера (market-ордер), % от notional |
| `maker_fee_pct` | 0.02 | Комиссия мейкера (лимитный reduce-only ордер), % от notional |
| `backtest_slippage_pct` | 0.3 | Допущение на проскальзывание входа в бэктесте, % (0 = выкл) |
| `pending_entry_pullback_pct` | 1.5 | Вход лимитником на откате от цены сигнала, % (0 = выкл — market сразу) |
| `pending_entry_timeout_minutes` | 9.0 | Через сколько минут снять неисполненный лимитник входа |

### Circuit Breaker (защита от серий убытков)

Встроен в `PositionManager`. Отслеживает количество убыточных сделок подряд. При достижении порогов:

1. **2 убытка подряд** (`circuit_breaker_loss_streak_reduce`) → размер позиции уменьшается до `circuit_breaker_reduce_mult_pct`% (по умолчанию 50%)
2. **3 убытка подряд** (`circuit_breaker_loss_streak_stop`) → полная остановка торговли на `circuit_breaker_stop_minutes` минут (по умолчанию 60)
3. **Любая прибыльная сделка** → сброс счётчика, возобновление нормальной торговли

Статус `circuit_breaker_stop` возвращается `open_position()` и записывается в `signals.missed_reason`. Бэктест-раннер симулирует ту же логику.

### Расчёт позиции

- **Стоп-лосс**: `sl = entry × (1 − stop_loss_pct / 100)` — фиксированный % от цены входа
- **Тейк-профит**: `tp = entry + (entry × stop_loss_pct / 100) × risk_reward_ratio` (например +15% при SL=5%, ratio=3.0)
- **Размер позиции**: `quantity = risk_budget / sl_distance`, где `risk_budget = баланс × risk_per_trade_pct / 100`
  - Риск в долларах фиксирован относительно депозита
  - Множитель рыночного режима применяется к `risk_budget` (CAUTIOUS = ×0.5)

## ИИ-режим (оркестратор-скилл + сабагенты, отдельный аккаунт)

**Доп. режим, не заменяет алгоритм** (`agent.enabled: false` по умолчанию — при выключенном
режиме код бота работает байт-в-байт как раньше). Мотивация — гипотеза, что часть входов
алгоритма систематически запоздалые (см. `pending_entry_pullback_pct` выше); ИИ-режим — способ
проверить, добавляет ли LLM-контекст (funding, стакан, история монеты, старшие таймфреймы),
которого алгоритм не видит, ценность поверх чисто механических фильтров.

**Архитектура — параллельный пайплайн, не вето поверх алгоритма.** Оба пайплайна получают одни
и те же сигналы от `SetupDetector`, но исполняются НЕЗАВИСИМО:
- **algo** — `self._positions`, текущий `_trading_connector` (основной аккаунт), не изменился.
- **agent** — `self._agent_positions`, ОТДЕЛЬНЫЙ аккаунт биржи (`agent.exchange`/`api_key`/`secret`
  в конфиге, отдельные переменные окружения — не путать с `trading.*`). Изоляция риска — сам факт
  отдельного аккаунта, а не только `dry_run`.

`PositionManager` получил параметр `source` (`"algo"` / `"agent"`) — все запросы/лимиты/кулдауны
(`_count_open`, `_has_position`, `_in_cooldown`, `update_positions`, `check_pending_entries`)
скоуплены по `source`, поэтому два пайплайна не видят и не блокируют друг друга, даже сидя в одной
таблице `trades`.

**Python не вызывает LLM сам вообще** (изменено после того, как выяснилось: pay-per-token
Anthropic API недоступен, есть только подписка Claude, которая работает через Claude Code, а не
через прямой API-ключ). Решения принимает **оркестратор** — автономная `/loop`-сессия Claude
Code по скиллу `.claude/skills/vibetrade-agent-loop`, которую пользователь запускает вручную и
держит открытой (для начала — приемлемо, обсуждалось явно). Она сама следит за новыми сигналами
и открытыми сделками агент-пайплайна, спавнит сабагентов `entry-agent`/`reeval-agent` (тот же
`Agent`-тул, которым Claude Code вызывает любых сабагентов), применяет их вердикт и **рассказывает
текстом**, что сделала за цикл — это и есть единственный канал видимости (pull, без
пуш-уведомлений — по решению пользователя).

**`agent.enabled=true` полностью отключает Telegram во всём приложении**, не только для
ИИ-пайплайна. `Application.start()` не поднимает ни основной `TelegramNotifier`, ни
`telegram_price_surge` — значит, ни команды (`/status`, `/pause`, `/positions`, ...), ни
уведомления algo-пайплайна (открытие/закрытие сделок, Circuit Breaker, смена рыночного режима),
ни сигналы `PriceSurgeDetector` никуда не отправляются, пока ИИ-режим включён. Единственный канал
видимости в этом состоянии — беседа оркестратора (см. выше) и прямые запросы к
`data/trading_bot.db`. Чтобы вернуть Telegram — выключить `agent.enabled` и перезапустить бота.

### Три уровня разделения обязанностей
1. **Python-бот** (`src/core/app.py`, `src/executor/position_manager.py`) — генерирует сигналы
   (не меняется), механически синхронизирует agent-пайплайн с биржей (`_agent_position_loop`:
   TP/SL-синк, pending-входы) и держит быстрый опрос цены наблюдаемых монет
   (`_agent_watch_loop`). LLM не вызывает.
2. **Сабагенты** `.claude/agents/entry-agent.md` / `reeval-agent.md` — только читают данные
   (`tools: Bash`, разрешён исключительно `python scripts/agent_data.py <tool> '<json>'`) и
   выносят вердикт текстом/JSON. Не имеют доступа к исполнению — не могут сами открыть/закрыть
   сделку.
3. **Оркестратор** (скилл `vibetrade-agent-loop`) — единственное место, где вердикт сабагента
   реально применяется: вызывает `scripts/agent_actions.py <action> <decision.json>`, который
   дёргает `apply_agent_*`/`open_position` в `AgentPositionManager`
   (`src/executor/agent_position_manager.py` — наследник `PositionManager`, куда вынесено ВСЁ
   решаемое агентом поведение, чтобы не трогать код алго-режима при его расширении) и пишет
   строку в `agent_decisions`.

### Strategy briefing: полная картина стратегии, не только общие рекомендации
`build_strategy_briefing()` (`src/agent/tools.py`, обёрнута в `scripts/agent_briefing.py`)
собирает динамический блок из ЖИВОГО конфига (`StrategyConfig`/`TradingConfig`, а не хардкод
текстом в `.claude/agents/*.md` — иначе разойдётся при правке `config.yaml`): реальные пороги
детектора (`volume_surge_mult`, `sustain_bars`, `oi_slope_min_pct`, диапазон роста цены,
антиспайк/exhaustion), реальные риск-параметры этого аккаунта (`stop_loss_pct`,
`risk_reward_ratio`, `leverage`, `partial_close_pct`, `max_hold_hours`,
`pending_entry_pullback_pct`), пороги Circuit Breaker и явное упоминание известной проблемы
позднего входа (см. `pending-entry-pullback-sweep-july-2026`). Оркестратор вызывает скрипт раз
за цикл и вставляет вывод текстом в промпт сабагенту.

### Вход
Оркестратор находит сигналы без записи `kind='entry'` в `agent_decisions` не старше ~15 минут
(детали — в самом скилле), спавнит `entry-agent` с briefing + деталями сигнала. Сабагент сам
решает, какие инструменты вызвать (funding rate, сводка стакана, тренд OI, история пампов
монеты, рыночный контекст, старшие таймфреймы, активность сигналов по другим монетам — прокси
секторальной ротации), отвечает `{"approve": bool, "entry_mode": "limit"|"market",
"pullback_pct": float, "reasoning": str}` — `entry_mode`/`pullback_pct` опциональны (по
умолчанию лимитник на конфиговом откате, как у алго-режима); если `entry_mode="limit"`, сам
откат агент может выбрать по глубине стакана, код клэмпит его в
`agent.entry_pullback_min_pct`..`max_pct`. Оркестратор передаёт это в `scripts/agent_actions.py
open_entry` — скрипт сам учитывает `agent.dry_run` (при `true` решение пишется в
`agent_decisions`, сделка не открывается даже на изолированном аккаунте) и `entry_gate_enabled`.

### Ручной запрос пользователя
Пользователь может в любой момент попросить оркестратора (прямо в беседе, не по расписанию)
проверить конкретную монету, которую заметил сам, а детектор сигнал ещё не дал/отфильтровал.
Оркестратор сам (без сабагента, бесплатно) делает первичную проверку через `agent_data.py`
(`get_symbol_snapshot`/`get_oi_trend` — есть ли объективный рост цены/объёма) и только если
похоже на реальный сетап — вызывает `scripts/agent_actions.py create_manual_signal`, который
вставляет строку в `signals` в обход детектора (`setup_type="manual"`, `direction="long"`
всегда — система long-only). Дальше сигнал идёт по тому же пути, что обычный: briefing →
`entry-agent` → `open_entry` с полученным `signal_id`, без каких-либо изменений в остальном
пайплайне. `create_manual_signal` сам добавляет в `message` пометку
`[РУЧНОЙ ЗАПРОС — детектор объём/OI/цену не проверял]` — `entry-agent.md` учит распознавать эту
пометку и в этом случае сначала проверять сам факт движения (объём/OI), а не только контекст
как обычно, поскольку обычное допущение "детектор уже подтвердил объём/OI/цену" здесь не
выполняется. Подробности процесса — `.claude/skills/vibetrade-agent-loop/SKILL.md`, "Ручной
запрос пользователя".

### Сопровождение
Оркестратор находит сделки agent-пайплайна (`status IN ('open','pending')`) без свежей
`reeval`-записи за `agent.reeval_interval_minutes`, спавнит `reeval-agent` с briefing +
`trade_id`. Сабагент вызывает `get_open_position` первым делом — оно возвращает `status`,
определяющий, какая ветка действий уместна:

**`status="open"`** — текущий стоп/тейк/PnL, ответ `hold`/`tighten_sl`/`raise_tp`/
`partial_close`/`extend_hold`/`close`:
- **`tighten_sl`** → `apply_agent_tighten_sl` — переиспользует `set_tpsl()` (тот же механизм, что
  и перевод в безубыток). Жёсткий рельс **в коде**, не только в промпте: сравнивает с
  `Trade.current_sl_price` (последний известный эффективный стоп) и отклоняет любую попытку
  ослабить SL.
- **`raise_tp`** → `apply_agent_raise_tp` — симметрично `tighten_sl`: сравнивает с
  `Trade.current_tp_price` (или формульным TP, если ещё не переставлялся) и отклоняет попытку
  понизить тейк.
- **`partial_close`** → `apply_agent_partial_close` — фиксирует 50% по рынку немедленно (тот же
  процент, что у автоматического триггера `partial_close_pct`, просто не дожидаясь его), не
  трогая SL — это отдельное независимое решение `tighten_sl`. Не сработает повторно, если
  `Trade.partial_closed` уже `true`.
- **`extend_hold`** → `apply_agent_hold_extension` — двигает `Trade.llm_hold_until`, который
  `_check_time_exit` учитывает как `max(механический_дедлайн, llm_hold_until)` — агент может
  ТОЛЬКО продлить удержание, никогда не сократить. Капается конфигом и за раз
  (`max_hold_extension_hours`), и суммарно на сделку (`max_hold_extension_total_hours`, счётчик —
  `Trade.llm_hold_extension_total_hours`).
- **`close`** → `apply_agent_close` — снимает висящий лимитник частичной фиксации
  (`cancel_all_orders`) и закрывает по рынку, `reason="llm_close"`.

**`status="pending"`** (лимитник ещё не исполнился) — вместо PnL/TP/SL отдаётся
`limit_price`/`distance_to_fill_pct`/`minutes_until_expiry`, ответ
`keep_pending`/`reprice`/`enter_market`/`cancel_pending`:
- **`reprice`** → `apply_agent_reprice_pending` — снимает старый лимитник, ставит новый на
  свежей цене с новым откатом, сбрасывает таймаут заново.
- **`enter_market`** → `apply_agent_convert_to_market` — снимает лимитник, входит по рынку тем
  же объёмом, дальше стандартная настройка TP/SL и лимитника частичной фиксации (общий с
  механическим путём `_setup_tp_sl_and_partial`).
- **`cancel_pending`** → `apply_agent_cancel_pending` — снимает лимитник, `status="cancelled"`
  (отдельно от механического `expired` — видно, где агент сам отказался от сетапа).

Все действия (кроме `open_entry`) перед изменением перепроверяют состояние НА БИРЖЕ — не только
`Trade.status` в БД, — потому что механический `_agent_position_loop` опрашивает биржу
независимо и может исполнить/снять/закрыть сделку, пока идёт LLM round-trip
(`agent_actions.py._verify_open`/`_verify_pending`; см. `AgentPositionManager.
_exchange_has_open_position`). При расхождении действие просто не применяется (`applied: false`
в ответе `agent_actions.py`) — это ожидаемо, не ошибка.

Штатные биржевые TP/SL остаются главной защитой независимо от исхода работы агента — если
оркестратор упал/сессия закрыта/сабагент ошибся, позиция всё равно защищена резидентным
стоп-лоссом на бирже.

### Механические таски бота (`app.py`) — не зависят от цикла сканирования рынка
Полный цикл сканирования всего рынка занимает несколько минут — слишком редко для сопровождения
конкретных открытых сделок. Два независимых `asyncio`-таска, оба стартуют в `Application.start()`
и останавливаются в `stop()`:
- `_agent_watch_loop` (`agent.watch_interval_seconds`, по умолчанию 30с) — опрашивает **только**
  монеты под наблюдением агента (его открытые/pending сделки) напрямую через `fetch_ticker`.
- `_agent_position_loop` (60с) — TP/SL-синхронизация с биржей и pending-входы для agent-пайплайна
  (`update_positions`/`check_pending_entries`). LLM здесь не участвует — это делает оркестратор.

Оба работают только при `agent.enabled=true` — при выключенном режиме не создают лишней нагрузки.

### Данные, которых раньше не было
`fetch_funding_rate`/`fetch_order_book_summary` — новые методы `ExchangeConnector`
(агрегаты — spread%, глубина в USD на ±0.5/1% от mid, **не сырые уровни стакана**, чтобы не
раздувать контекст LLM шумом). Старшие таймфреймы для уровней поддержки/сопротивления —
`get_higher_timeframe_history` дёргает биржу напрямую по требованию (не хранится в БД).
`AgentToolkit._tool_get_market_context` читает последний снимок `MarketContextSnapshot` из БД
(пишется ботом каждый цикл) — не требует живого подключения к TradingView.

### Конфигурация (`config.yaml → agent`)

| Параметр | По умолчанию | Смысл |
|----------|-------------|-------|
| `enabled` | `false` | Включить ИИ-режим |
| `dry_run` | `true` | Логировать решения, не открывать реальные сделки даже на своём аккаунте |
| `exchange` / `api_key` / `secret` | — | Отдельный аккаунт биржи (не `trading.*`) |
| `model` | `sonnet` | Модель Claude для сабагентов entry-agent/reeval-agent |
| `reeval_interval_minutes` | 20.0 | Раз во сколько минут переоценивать одну позицию (сверяет оркестратор) |
| `watch_interval_seconds` | 30 | Раз во сколько секунд обновлять цену наблюдаемых монет |
| `max_hold_extension_hours` / `_total_hours` | 12.0 / 24.0 | Кап продления удержания, за раз / суммарно |
| `allow_sl_tighten` / `allow_early_close` | `true` | Разрешить соответствующее действие (ослабление SL запрещено всегда) |
| `allow_raise_tp` | `true` | Разрешить поднимать тейк (опустить нельзя никогда) |
| `allow_partial_close` | `true` | Разрешить фиксировать часть позиции по рынку до авто-триггера |
| `allow_pending_management` | `true` | Разрешить двигать/переводить в market/отменять свой неисполненный лимитник входа |
| `entry_pullback_min_pct` / `max_pct` | 0.5 / 4.0 | Диапазон отката для лимитника входа, который может выбрать агент (клэмп в коде) |
| `daily_call_budget` | 200 | Максимум запусков сабагентов в сутки (оркестратор сверяет с числом строк `agent_decisions` за сегодня) |

### ⚠️ Доступ к БД только через `docker exec`, никогда напрямую с хоста
Бот работает в Docker-контейнере (`docker-compose.yml`, `container_name: trading-bot`), `data/`
внутри контейнера — **named Docker volume** (`vibetrade_data`), не bind-mount с хоста — то есть
хостовый `sqlite3 data/trading_bot.db`/`python scripts/agent_*.py` в принципе не видит файл бота
(это специально: старый bind-mount дважды приводил к порче БД, см. "База данных" ниже). **Все
обращения к БД — только `docker exec trading-bot <команда>`** (Dockerfile копирует `scripts/` в
образ специально для этого). Скилл и оба сабагента уже написаны с этим требованием — не убирать
обёртку при правке.

### Файлы
- `src/agent/tools.py` — `AgentToolkit` (данные) + `build_strategy_briefing()` + `AGENT_VERSION`
- `src/executor/agent_position_manager.py` — `AgentPositionManager(PositionManager)`, все
  `apply_agent_*` (решаемое агентом поведение, изолировано от кода алго-режима)
- `scripts/agent_data.py` — CLI для сабагентов (только чтение, `AgentToolkit.dispatch`)
- `scripts/agent_briefing.py` — печатает strategy briefing
- `scripts/agent_actions.py` — CLI для оркестратора (единственное место, где решение
  применяется: `open_entry`/`tighten_sl`/`raise_tp`/`partial_close`/`extend_hold`/`close`/
  `reprice_pending`/`enter_market`/`cancel_pending`, пишет `agent_decisions`)
- `.claude/agents/entry-agent.md`, `.claude/agents/reeval-agent.md` — сабагенты-судьи
- `.claude/skills/vibetrade-agent-loop/SKILL.md` — оркестратор (автономный `/loop`)

### Статус на момент внедрения (июль 2026)
Включено с `enabled=false`, `dry_run=true` по умолчанию — режим ещё не запускался "в бою".
Валидировать на исторических БД нельзя: funding rate и стакан не сохранены в прошлых данных —
качество решений можно оценить только вперёд, по новым логам `agent_decisions`. Версия инструкций
сабагентов/скилла логируется в `agent_decisions.agent_version` (`AGENT_VERSION` в
`src/agent/tools.py`) — при значимой правке `.claude/agents/*.md` или скилла следует её
увеличивать, чтобы позже можно было сопоставить качество решений с конкретной редакцией.

## База данных

SQLite в **WAL-режиме** (`data/trading_bot.db` внутри контейнера, на named Docker volume
`vibetrade_data` — не bind-mount с хоста). Миграции: Alembic + ручной `ALTER TABLE` в `init_db()`
для обратной совместимости.

**История (21-22.07.2026, для контекста будущих инцидентов)**: изначально `data/` была
bind-mount (`./data:/app/data`) — WAL полагается на shared-memory индекс (`-shm`) через `mmap`
для координации между соединениями, а это ненадёжно на bind-mount через osxfs/gRPC-FUSE Docker
Desktop for Mac, независимо от того, кто пишет: хост или несколько соединений внутри одного
контейнера. Это дважды привело к порче БД (`market_context_snapshots`, затем индекс
`ix_candles_symbol`) — второй раз, судя по всему, из-за возросшего числа конкурентных SQLite-
соединений внутри контейнера после включения `agent.enabled` (`_agent_watch_loop`/
`_agent_position_loop` — отдельные сессии поверх основного цикла сборщика). Временный фикс —
переключение на `DELETE`-режим (обычные файловые локи вместо mmap) — устранил порчу, но ценой
полной сериализации записи: основной цикл сборщика держит одну транзакцию на весь ~5-мин скан,
и конкурентные таски ИИ-режима немедленно ловили `database is locked`. **Финальный фикс**:
`data/` переведена в named Docker volume (`docker-compose.yml`) — хранится в файловой системе
Docker VM напрямую, не через host-bridge, поэтому `mmap` работает штатно и WAL восстановлен.
Хостовый `data/` теперь содержит только исторические БД для бэктестов
(`trading_bot_*.db`) и такой же снапшот `trading_bot.db.pre-named-volume-snapshot-*` — актуальный
файл живёт только в volume, доступен через `docker exec`/`docker cp`, не напрямую с хоста.

**Модели:**

| Таблица | Назначение |
|---------|-----------|
| `tickers` | Последний тикер (цена, объём) — exchange + symbol |
| `candles` | OHLCV-свечи — уникальность по exchange + symbol + timestamp |
| `open_interest` | OI — сохраняется только при изменении значения |
| `signals` | Сигналы основной стратегии, включает `missed_reason` (причина пропуска) |
| `price_surge_signals` | Сигналы PriceSurgeDetector |
| `filtered_signals` | Сетапы, отсеянные `SetupDetector` до появления в `signals` (см. ниже) |
| `trades` | Торговые позиции (вход/выход, PnL, partial close, TP/SL статус), `source` — 'algo'/'agent' |
| `agent_decisions` | Решения ИИ-агента (вход/сопровождение) — verdict, reasoning, полный трейс вызовов инструментов |
| `market_context_snapshots` | Снимки рыночного контекста (regime, trend, Supertrend, BTC/OTHERS) |

### Аудит отфильтрованных сетапов (`filtered_signals`)

`SetupDetector.analyze()` (`src/analytics/detector.py`) пишет строку в `filtered_signals`
(exchange, symbol, stage, reason, timestamp) каждый раз, когда сетап **уже прошёл основной
порог всплеска объёма**, но был отсеян одной из последующих проверок — OI-тренд или ценовой
паттерн. Отсюда прямо видно, какая монета и на каком фильтре срезалась (раньше это было видно
только из текстовых логов `logger.info`, и то не всегда — часть веток вообще молчала, а те, что
логировали, не писали symbol).

`stage` — один из: `volume_spike`, `volume_dump`, `volume_fading`, `volume_declining`,
`oi_declining`, `oi_slope_low`, `pre_surge_pump`, `hourly_drop`, `price_growth_low`,
`exhaustion`, `exhaustion_extreme`, `price_growth_high`.

Намеренно **не** логируются монеты, которые даже не приблизились к порогу объёма (подавляющее
большинство из ~450 сканируемых каждый цикл) — это шум, не сетап. `check_volume_pattern` /
`check_price_trend` / `_check_oi_trend` принимают опциональный `context: dict | None`, который
заполняется `stage`/`reason` перед `return False`/`None`; публичные сигнатуры остались
обратно совместимыми (backtest/runner.py и тесты зовут их без `context`).

Пример запроса — что и почему срезалось за сегодня:
```sql
SELECT stage, symbol, reason, timestamp FROM filtered_signals
WHERE date(timestamp) = date('now') ORDER BY timestamp DESC;
```

## Telegram-боты

Два независимых бота (основной + price surge), каждый со своим токеном. **Важно:** токены должны быть разными, иначе `TelegramConflictError`.

**Оба полностью отключаются при `agent.enabled: true`** — см. раздел "ИИ-режим" выше.

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
- **Комиссии и slippage учтены только в `backtest/runner.py`**, не в скриптах `scripts/*sweep*.py`/`analyze_performance.py` — у них свои независимые копии цикла симуляции (историческая дупликация кода). При очередном прогоне свипа параметров через эти скрипты результаты по-прежнему будут gross-of-fee — учитывай это при интерпретации, либо перенеси тот же fee/slippage-патч из `runner.py` вручную перед запуском.

## Конфигурация и секреты

- **Конфиг**: `config/config.yaml` (YAML + подстановка `${ENV_VAR}` из `.env`)
- **Секреты**: `.env` (не коммитится), содержит токены API и Telegram
- **Две стратегии**: `strategy` (основная, с торговлей) и `strategy_price_surge` (только сигналы)
- **Два Telegram-бота**: `telegram` и `telegram_price_surge` (независимые токены)
- **ИИ-режим** (если `agent.enabled: true`): API-ключ Anthropic НЕ нужен — LLM вызывается через Claude Code (подписка), см. раздел "ИИ-режим". Нужны только `AGENT_BYBIT_API_KEY`/`AGENT_BYBIT_SECRET` — ключи ОТДЕЛЬНОГО аккаунта биржи (не путать с `trading.*`). Плюс запущенная и держащаяся открытой `/loop`-сессия по скиллу `vibetrade-agent-loop` — это не переменная окружения, а отдельный процесс, который пользователь стартует сам

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
- **Новый фильтр в детекторе** — добавить метод в `SetupDetector`, вызвать из `check_price_trend` или `check_volume_pattern`. Добавить параметры в `StrategyConfig` с дефолтом 0 (= выкл). Если фильтр отсекает near-miss (сетап уже прошёл порог объёма) — заполнить `context["stage"]`/`context["reason"]` перед `return`, чтобы отказ попал в `filtered_signals` (см. "Аудит отфильтрованных сетапов").
- **Новый сервис-обработчик** — по аналогии с `PriceSurgeSignalProcessor`: инкапсулирует обогащение сигналов, persistence и нотификации.
- **Новая биржа** — добавить `ExchangeConfig` в `config.yaml`, ccxt поддерживает её из коробки
- **Нотификации в другой канал** — реализовать аналог `TelegramNotifier` с тем же интерфейсом
- **Новый инструмент для ИИ-агента** — добавить метод `_tool_*` в `AgentToolkit` (`src/agent/tools.py`) и упомянуть его в `.claude/agents/entry-agent.md`/`reeval-agent.md` (список доступных `<tool_name>` для `scripts/agent_data.py`). Инструмент должен отдавать агрегированные метрики, не сырые API-дампы (экономия контекста LLM)
