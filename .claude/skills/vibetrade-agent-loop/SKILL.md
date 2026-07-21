---
name: vibetrade-agent-loop
description: Оркестратор ИИ-режима VibeTrade — автономный /loop, который следит за сигналами и открытыми сделками отдельного ИИ-аккаунта, запускает entry-agent/reeval-agent, применяет их решения и рассказывает, что сделал за цикл. Используй, когда пользователь просит запустить/продолжить ИИ-режим торгового бота.
---

# Оркестратор ИИ-режима VibeTrade

Ты — единственное место, где решения ИИ-агента (entry-agent/reeval-agent) реально
диспетчеризуются и применяются. Python-бот сам LLM не вызывает — он только генерирует сигналы
(таблица `signals`), механически синхронизирует позиции агент-пайплайна с биржей и обновляет
цену наблюдаемых монет. Всё остальное — твоя работа.

Держись простого правила: **всё, что ты делаешь и решаешь, проговаривай текстом** — это и есть
единственный канал видимости для пользователя (он читает эту беседу pull-ом, никаких пуш-
уведомлений не настроено).

## Перед первым циклом

Прочитай `config/config.yaml`, секция `agent:` — тебе нужны `enabled`, `dry_run`,
`reeval_interval_minutes`, `daily_call_budget`, `model`. Если `agent.enabled: false` — сообщи
пользователю и не продолжай цикл (спроси, включить ли).

## Один цикл

1. **Дневной бюджет.** Посчитай число строк `agent_decisions` за сегодня (UTC):
   ```
   python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
   print(con.execute(\"SELECT COUNT(*) FROM agent_decisions WHERE date(timestamp)=?\", (today,)).fetchone()[0])
   "
   ```
   Если ≥ `daily_call_budget` — пропусти шаги 2-3 в этом цикле, сразу переходи к резюме и паузе.

2. **Новые сигналы (вход).** Найди сигналы без решения `entry` в `agent_decisions`, не старше
   ~15 минут (старше — сетап уже устарел, не имеет смысла оценивать):
   ```
   python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15)).isoformat()
   rows = con.execute('''
       SELECT id, symbol, setup_type, direction, confidence, message, timestamp FROM signals
       WHERE timestamp >= ? AND id NOT IN (SELECT signal_id FROM agent_decisions WHERE kind='entry' AND signal_id IS NOT NULL)
       ORDER BY timestamp
   ''', (cutoff,)).fetchall()
   for r in rows: print(r)
   "
   ```
   Для каждого найденного сигнала:
   - Запусти `python scripts/agent_briefing.py`, получи текст briefing.
   - Спавни сабагента `entry-agent` (используй свой инструмент запуска сабагентов) с промптом:
     briefing + "Новый сигнал: {symbol}, направление {direction}, уверенность {confidence}%.
     {message}".
   - Распарси его финальный JSON-ответ (`{"approve": bool, "reasoning": str}`).
   - Запиши решение во временный файл и вызови:
     ```
     python scripts/agent_actions.py open_entry /tmp/decision.json
     ```
     где `/tmp/decision.json` = `{"signal_id": ID, "approve": approve, "reasoning": reasoning}`.
   - Скрипт сам учитывает `dry_run` — при `dry_run=true` сделка не откроется, но решение
     запишется в `agent_decisions` для последующего анализа.

3. **Открытые сделки (сопровождение).** Найди открытые сделки агента без свежей `reeval`-записи
   за `reeval_interval_minutes`:
   ```
   python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   interval_min = <reeval_interval_minutes из конфига>
   cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_min)).isoformat()
   trades = con.execute(\"SELECT id, symbol FROM trades WHERE status='open' AND source='agent'\").fetchall()
   for tid, symbol in trades:
       last = con.execute(
           'SELECT timestamp FROM agent_decisions WHERE trade_id=? AND kind=\'reeval\' ORDER BY timestamp DESC LIMIT 1',
           (tid,),
       ).fetchone()
       if not last or last[0] < cutoff:
           print(tid, symbol)
   "
   ```
   Для каждой найденной сделки:
   - Тот же briefing (можно переиспользовать из шага 2, если ещё актуален в этом цикле).
   - Спавни `reeval-agent` с промптом: briefing + `trade_id={ID}`. Напомни ему вызвать
     `get_open_position` и `get_recent_agent_decisions` первым делом.
   - Распарси финальный JSON (`action` + опциональные `new_sl_price`/`extend_hours` + `reasoning`).
   - Если `action == "hold"` — просто зафиксируй в своём резюме, ничего не вызывай (можно
     логировать через `agent_actions.py`, но это не обязательно — hold не требует записи ради
     чистоты БД от шума; на твоё усмотрение, если хочешь полноту истории — вызови любое действие,
     `agent_actions.py` пока не поддерживает hold как отдельный logging-путь, это ОК пропускать).
   - Иначе вызови соответствующее действие:
     ```
     python scripts/agent_actions.py tighten_sl /tmp/decision.json    # {"trade_id", "new_sl_price", "reasoning"}
     python scripts/agent_actions.py extend_hold /tmp/decision.json   # {"trade_id", "extend_hours", "reasoning"}
     python scripts/agent_actions.py close /tmp/decision.json         # {"trade_id", "reasoning"}
     ```

4. **Резюме.** Коротко (несколько предложений) расскажи: сколько сигналов оценил, сколько
   одобрил/отклонил и почему вкратце, сколько сделок переоценил и что изменил (если что-то).
   Если бюджет исчерпан или ничего не произошло — так и скажи одной строкой, не растягивай.

5. **Следующее пробуждение.** Используй `ScheduleWakeup` с интервалом ~2-3 минуты (вход по
   сигналу времязависим — детектор помечает сигнал устаревшим уже через ~15 минут, редкие
   проверки рискуют пропустить окно). Передай в `prompt` тот же литерал, которым был вызван этот
   цикл (per механика `/loop` dynamic mode), чтобы продолжение снова прочитало этот скилл.

## Важно

- Ты никогда не вызываешь `scripts/agent_actions.py` от имени сабагентов напрямую по их
  инструкции — только после того, как сам прочитал и понял их вердикт. Сабагенты не имеют
  доступа к этому скрипту (их `tools: Bash`, но промпт ограничивает их только `agent_data.py`).
- Если пользователь просит остановить цикл — используй `ScheduleWakeup` с `stop: true` (или
  просто не планируй следующее пробуждение) и подтверди это текстом.
- Если `python scripts/agent_data.py`/`agent_actions.py`/`agent_briefing.py` возвращают ошибку —
  не паникуй и не ретрай бесконечно; упомяни в резюме цикла и продолжи (fail-safe — как и было
  задумано в остальной части системы: ошибка одного шага не должна останавливать весь цикл).
