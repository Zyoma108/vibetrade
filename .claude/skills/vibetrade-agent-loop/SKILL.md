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

**Критически важно: ВСЕГДА обращайся к `data/trading_bot.db` только через `docker exec
trading-bot ...`, никогда напрямую с хоста** (ни `sqlite3 data/trading_bot.db ...`, ни
`python3 -c ...`, ни `python scripts/agent_*.py ...` без `docker exec` спереди). Бот работает в
Docker-контейнере, `data/` — bind mount с хоста. Если читать/писать файл одновременно с хоста и
из контейнера, блокировки SQLite не гарантированно согласуются через границу Docker Desktop —
именно так один раз уже была повреждена база (`market_context_snapshots`, "database disk image
is malformed", 21.07.2026). Все команды ниже уже написаны с `docker exec trading-bot` — не
убирай эту обёртку.

## Перед первым циклом

Прочитай `config/config.yaml`, секция `agent:` — тебе нужны `enabled`, `dry_run`,
`reeval_interval_minutes`, `daily_call_budget`, `model`, `entry_symbol_cooldown_minutes`. Если
`agent.enabled: false` — сообщи пользователю и не продолжай цикл (спроси, включить ли).

## Один цикл

1. **Дневной бюджет.** Посчитай число строк `agent_decisions` за сегодня (UTC):
   ```
   docker exec trading-bot python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
   print(con.execute(\"SELECT COUNT(*) FROM agent_decisions WHERE date(timestamp)=?\", (today,)).fetchone()[0])
   "
   ```
   Если ≥ `daily_call_budget` — пропусти шаги 2-3 в этом цикле, сразу переходи к резюме и паузе.

2. **Новые сигналы (вход).** Найди сигналы без решения `entry` в `agent_decisions`, не старше
   ~15 минут (старше — сетап уже устарел, не имеет смысла оценивать), И по монетам, для которых
   не было `entry`-решения за последние `entry_symbol_cooldown_minutes`. Важно: `signals.timestamp`
   хранится как naive-строка вида `2026-07-22 07:47:05.768977` (пробел-разделитель, без `T` и
   без `+00:00`) — оба cutoff формируй через `strftime('%Y-%m-%d %H:%M:%S.%f')`, а НЕ через
   `.isoformat()` на aware-datetime: `isoformat()` даёт `T`-разделитель и суффикс `+00:00`, а при
   строковом сравнении SQLite `' ' < 'T'`, из-за чего `timestamp >= cutoff` ложно фейлится для
   ЛЮБОГО сигнала за тот же день (баг был найден и исправлен 22.07.2026 — сигнал по CL был
   пропущен из-за этого):
   ```
   docker exec trading-bot python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   cooldown_min = <entry_symbol_cooldown_minutes из конфига>
   fresh_cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S.%f')
   cooldown_cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=cooldown_min)).strftime('%Y-%m-%d %H:%M:%S.%f')
   rows = con.execute('''
       SELECT id, symbol, setup_type, direction, confidence, message, timestamp FROM signals
       WHERE timestamp >= ?
         AND id NOT IN (SELECT signal_id FROM agent_decisions WHERE kind='entry' AND signal_id IS NOT NULL)
         AND symbol NOT IN (SELECT symbol FROM agent_decisions WHERE kind='entry' AND timestamp >= ?)
       ORDER BY timestamp
   ''', (fresh_cutoff, cooldown_cutoff)).fetchall()
   for r in rows: print(r)
   "
   ```
   Монета, отфильтрованная кулдауном, просто пропускается в этом цикле — не трать на неё вызов
   entry-agent, но можешь упомянуть в резюме одной строкой, что сигнал был, но монета на кулдауне
   (памятка: 22.07.2026 ALICE дала 3 почти идентичных сигнала за 12 минут, каждый пришлось гонять
   через entry-agent — кулдаун избавляет от этого).

   Для каждого найденного сигнала:
   - Запусти `docker exec trading-bot python scripts/agent_briefing.py`, получи текст briefing.
   - Спавни сабагента `entry-agent` (используй свой инструмент запуска сабагентов) с промптом:
     briefing + "Новый сигнал: {symbol}, направление {direction}, уверенность {confidence}%.
     {message}".
   - Распарси его финальный JSON-ответ (`{"approve": bool, "reasoning": str}`).
   - Запиши решение во временный файл ВНУТРИ контейнера (`docker exec trading-bot sh -c
     "cat > /tmp/decision.json <<'EOF'\n{...}\nEOF"`, либо `docker cp` файла с хоста в
     `trading-bot:/tmp/decision.json` — оба варианта кладут файл туда, откуда его увидит
     процесс внутри контейнера) и вызови:
     ```
     docker exec trading-bot python scripts/agent_actions.py open_entry /tmp/decision.json
     ```
     где `decision.json` = `{"signal_id": ID, "approve": approve, "reasoning": reasoning}`.
   - Скрипт сам учитывает `dry_run` — при `dry_run=true` сделка не откроется, но решение
     запишется в `agent_decisions` для последующего анализа.

3. **Открытые сделки (сопровождение).** Найди открытые сделки агента без свежей `reeval`-записи
   за `reeval_interval_minutes`:
   ```
   docker exec trading-bot python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   interval_min = <reeval_interval_minutes из конфига>
   cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_min)).strftime('%Y-%m-%d %H:%M:%S.%f')
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
   - Иначе положи решение в `decision.json` внутри контейнера (см. шаг 2) и вызови
     соответствующее действие:
     ```
     docker exec trading-bot python scripts/agent_actions.py tighten_sl /tmp/decision.json    # {"trade_id", "new_sl_price", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py extend_hold /tmp/decision.json   # {"trade_id", "extend_hours", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py close /tmp/decision.json         # {"trade_id", "reasoning"}
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
  доступа к этому скрипту (их `tools: Bash`, но промпт ограничивает их только `agent_data.py`,
  и тоже через `docker exec trading-bot`, см. `.claude/agents/entry-agent.md`).
- Если пользователь просит остановить цикл — используй `ScheduleWakeup` с `stop: true` (или
  просто не планируй следующее пробуждение) и подтверди это текстом.
- Если `agent_data.py`/`agent_actions.py`/`agent_briefing.py` возвращают ошибку — не паникуй и
  не ретрай бесконечно; упомяни в резюме цикла и продолжи (fail-safe — как и было задумано в
  остальной части системы: ошибка одного шага не должна останавливать весь цикл).
- Если `docker exec trading-bot` сам не работает (контейнер не запущен/не называется
  `trading-bot`) — остановись и сообщи пользователю текстом, не пытайся обойти это обращением к
  файлу напрямую с хоста (см. предупреждение выше про порчу БД).
