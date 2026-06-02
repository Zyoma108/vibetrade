# VibeTrade

Асинхронный торговый бот для криптобирж. Детектит пампинг-сетапы по аномальному объёму и росту открытого интереса, управляет позициями (virtual/real), отправляет сигналы в Telegram.

## Быстрый старт

```bash
# Установка
python -m venv .venv
source .venv/bin/activate
pip install -e .

# Конфигурация
cp config/config.example.yaml config/config.yaml
cp .env.example .env
# Заполни .env: BYBIT_API_KEY, BYBIT_SECRET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Запуск
make run              # реальная торговля (mode: real в конфиге)
make run-signal       # только сигналы, без торговли
make run-virtual      # виртуальная торговля (бумажный счёт)
```

## Бэктест

```bash
make backtest-load              # загрузить 7 дней истории
make backtest-run               # прогнать стратегию на истории
```

## Команды Telegram-бота

`/status` `/pause` `/resume` `/stats [day|week|month|all]` `/positions`

## Конфигурация

Основные параметры стратегии в `config/config.yaml → strategy`. Подробное описание — в [AGENTS.md](AGENTS.md).
