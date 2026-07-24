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
         AND symbol NOT IN (
             SELECT ad.symbol FROM agent_decisions ad
             WHERE ad.kind='entry' AND ad.timestamp >= ?
               AND (
                 ad.verdict != 'approve'
                 OR EXISTS (
                     SELECT 1 FROM trades t
                     WHERE t.signal_id = ad.signal_id AND t.status NOT IN ('expired', 'cancelled')
                 )
               )
         )
       ORDER BY timestamp
   ''', (fresh_cutoff, cooldown_cutoff)).fetchall()
   for r in rows: print(r)
   "
   ```
   Монета, отфильтрованная кулдауном, просто пропускается в этом цикле — не трать на неё вызов
   entry-agent, но можешь упомянуть в резюме одной строкой, что сигнал был, но монета на кулдауне
   (памятка: 22.07.2026 ALICE дала 3 почти идентичных сигнала за 12 минут, каждый пришлось гонять
   через entry-agent — кулдаун избавляет от этого).

   Важно: кулдаун держит монету только если решение было `reject`, ИЛИ `approve` реально привёл к
   сделке, которая всё ещё жива/была реальной (`pending`/`open`/`closed`) — НЕ держит, если
   `approve` кончился `expired` (лимитник не исполнился за `pending_entry_timeout_minutes` и снялся
   по таймауту) или `cancelled` (агент сам передумал по pending, см. `reeval-agent`). В этих двух
   случаях фактической сделки не было, и по возвращении новый сигнал по той же монете нужно
   оценивать заново, а не молчать до истечения `entry_symbol_cooldown_minutes` (баг, найден
   24.07.2026: `pending_entry_timeout_minutes` короче, чем `entry_symbol_cooldown_minutes`, поэтому
   лимитник снимался по таймауту раньше, чем истекал кулдаун решения — монета оставалась
   недоступной для переоценки ещё ~20 минут без реальной причины).

   Для каждого найденного сигнала:
   - Запусти `docker exec trading-bot python scripts/agent_briefing.py`, получи текст briefing.
   - Спавни сабагента `entry-agent` (используй свой инструмент запуска сабагентов) с промптом:
     briefing + "Новый сигнал: {symbol}, направление {direction}, уверенность {confidence}%.
     {message}".
   - Распарси его финальный JSON-ответ (`{"approve": bool, "entry_mode": "limit"|"market",
     "pullback_pct": float, "reasoning": str}` — `entry_mode`/`pullback_pct` опциональны, см.
     `.claude/agents/entry-agent.md`).
   - Запиши решение во временный файл ВНУТРИ контейнера (`docker exec trading-bot sh -c
     "cat > /tmp/decision.json <<'EOF'\n{...}\nEOF"`, либо `docker cp` файла с хоста в
     `trading-bot:/tmp/decision.json` — оба варианта кладут файл туда, откуда его увидит
     процесс внутри контейнера) и вызови:
     ```
     docker exec trading-bot python scripts/agent_actions.py open_entry /tmp/decision.json
     ```
     где `decision.json` = `{"signal_id": ID, "approve": approve, "entry_mode": entry_mode,
     "pullback_pct": pullback_pct, "reasoning": reasoning}` (поля `entry_mode`/`pullback_pct`
     передавай, только если сабагент их вернул — при их отсутствии в JSON срабатывает конфиговое
     поведение по умолчанию).
   - Скрипт сам учитывает `dry_run` и клэмпит `pullback_pct` в допустимый диапазон — при
     `dry_run=true` сделка не откроется, но решение запишется в `agent_decisions` для
     последующего анализа.

2b. **Ретрай истёкших лимитников — тот же сетап, не новый сигнал.** `status='expired'` значит
   только "цена не откатилась до лимитника за `pending_entry_timeout_minutes`" — это НЕ отработка
   сигнала и НЕ основание останавливаться или спрашивать пользователя. Пока сабагент явно не
   отказал (`approve: false`) и позиция так и не открылась — сетап остаётся живым, пробуй снова
   сам, каждый цикл, пока не откроется или не откажут (баг, найден 24.07.2026: на ORDER истекло
   подряд 2 лимитника без отката, и вместо повторной попытки оркестратор остановился и спросил
   пользователя, входить ли руками — неверно, входить надо было пытаться дальше самому). Найди
   монеты, чья ПОСЛЕДНЯЯ попытка входа (по `trades`, среди всех статусов, не только `expired`)
   закончилась `expired`, а не более свежим `open`/`pending`, и чьё последнее `entry`-решение —
   `approve` (не `reject`):
   ```
   docker exec trading-bot python3 -c "
   import sqlite3
   con = sqlite3.connect('data/trading_bot.db')
   rows = con.execute('''
       SELECT t.symbol, t.signal_id
       FROM trades t
       WHERE t.source='agent' AND t.status='expired'
         AND t.id = (SELECT MAX(id) FROM trades t2 WHERE t2.symbol = t.symbol AND t2.source='agent')
         AND (
             SELECT ad.verdict FROM agent_decisions ad
             WHERE ad.symbol = t.symbol AND ad.kind='entry'
             ORDER BY ad.timestamp DESC LIMIT 1
         ) = 'approve'
   ''').fetchall()
   for r in rows: print(r)
   "
   ```
   Условие `t.id = MAX(id)` само исключает монеты, которые шаг 2 уже успел переоткрыть в ЭТОМ ЖЕ
   цикле (у них появилась более новая запись `pending`/`open`, и она станет максимальной) — порядок
   важен, выполняй этот шаг ПОСЛЕ шага 2. Заметь: `status='expired'` появляется у сделки, только
   если лимитник реально был выставлен и не исполнился по таймауту — если `open_position` вообще
   отказал по гвардам (`risk_off`, `max_positions`, дубликат и т.п.), сделка не создаётся вовсе, и
   такая монета сюда никогда не попадёт (это не тот случай — гварды выше уровня "истёк лимитник").

   Для каждой найденной пары `(symbol, signal_id)`:
   - Подтяни исходные детали сигнала: `SELECT setup_type, direction, confidence, message FROM
     signals WHERE id=?` (детектор его не перегенерирует — переиспользуем тот же).
   - Дальше — тот же flow, что и в шаге 2 (тот же briefing, тот же `entry-agent`, тот же
     `open_entry` с ЭТИМ ЖЕ `signal_id`) — вызывать `open_entry` несколько раз подряд с одним
     `signal_id` не ломает ничего, уникальность по `signal_id` нигде не требуется.
   - Если `approve: false` — остановись, сабагент отказал; следующая проверка этого шага увидит
     `verdict='reject'` последним и сама пропустит монету, ничего запоминать вручную не нужно.
   - Если `approve: true`, но новый лимитник тоже не исполнится за `pending_entry_timeout_minutes`
     — на следующем цикле этот шаг найдёт монету снова (`status='expired'` у уже более свежей
     сделки) и повторит попытку сам. Не спрашивай пользователя, ждать ли новый сигнал или входить
     руками, пока это не входит в противоречие с дневным `daily_call_budget` (шаг 1) — это
     единственный естественный тормоз для этого цикла ретраев.

3. **Открытые и pending сделки (сопровождение).** Найди сделки агента (открытые ИЛИ ещё
   неисполненные лимитники на вход) без свежей `reeval`-записи за `reeval_interval_minutes`:
   ```
   docker exec trading-bot python3 -c "
   import sqlite3, datetime
   con = sqlite3.connect('data/trading_bot.db')
   interval_min = <reeval_interval_minutes из конфига>
   cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_min)).strftime('%Y-%m-%d %H:%M:%S.%f')
   trades = con.execute(\"SELECT id, symbol, status FROM trades WHERE status IN ('open','pending') AND source='agent'\").fetchall()
   for tid, symbol, status in trades:
       last = con.execute(
           'SELECT timestamp FROM agent_decisions WHERE trade_id=? AND kind=\'reeval\' ORDER BY timestamp DESC LIMIT 1',
           (tid,),
       ).fetchone()
       if not last or last[0] < cutoff:
           print(tid, symbol, status)
   "
   ```
   Для каждой найденной сделки (не важно, `open` или `pending` — `reeval-agent` сам определяет
   по `get_open_position.status`, какой набор действий уместен, см. `.claude/agents/
   reeval-agent.md`):
   - Тот же briefing (можно переиспользовать из шага 2, если ещё актуален в этом цикле).
   - Спавни `reeval-agent` с промптом: briefing + `trade_id={ID}`. Напомни ему вызвать
     `get_open_position` и `get_recent_agent_decisions` первым делом.
   - Распарси финальный JSON (`action` + поля по действию, см. `.claude/agents/reeval-agent.md`).
   - Если `action` — `hold` или `keep_pending` — зафиксируй в своём резюме И вызови:
     ```
     docker exec trading-bot python scripts/agent_actions.py hold /tmp/decision.json  # {"trade_id", "reasoning"}
     ```
     (verdict `hold` vs `keep_pending` скрипт определит сам по `Trade.status`, ничего в сделке не
     меняет). Это обязательно, а не по желанию: без записи cadence-проверка на шаге 3 (ищет
     последнюю `kind='reeval'` по `trade_id`) не увидит, что реэвал только что был, если твоя
     собственная память об этом потеряется (compaction, перезапуск `/loop`) — решит, что реэвала
     давно не было, и вызовет `reeval-agent` раньше срока.
   - Иначе положи решение в `decision.json` внутри контейнера (см. шаг 2) и вызови
     соответствующее действие:
     ```
     # status="open"
     docker exec trading-bot python scripts/agent_actions.py tighten_sl /tmp/decision.json      # {"trade_id", "new_sl_price", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py raise_tp /tmp/decision.json         # {"trade_id", "new_tp_price", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py partial_close /tmp/decision.json    # {"trade_id", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py extend_hold /tmp/decision.json      # {"trade_id", "extend_hours", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py close /tmp/decision.json            # {"trade_id", "reasoning"}
     # status="pending"
     docker exec trading-bot python scripts/agent_actions.py reprice_pending /tmp/decision.json  # {"trade_id", "new_pullback_pct", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py enter_market /tmp/decision.json     # {"trade_id", "reasoning"}
     docker exec trading-bot python scripts/agent_actions.py cancel_pending /tmp/decision.json   # {"trade_id", "reasoning"}
     ```
     Все действия (кроме `open_entry`) перед изменением перепроверяют состояние НА БИРЖЕ, не
     только в БД — если механический цикл бота уже исполнил/снял/закрыл сделку, пока шёл вызов
     сабагента, действие просто не применится (`applied: false` в ответе), это ожидаемо и не
     ошибка.

4. **Резюме.** Коротко (несколько предложений) расскажи: сколько сигналов оценил, сколько
   одобрил/отклонил и почему вкратце, сколько сделок переоценил и что изменил (если что-то).
   Если бюджет исчерпан или ничего не произошло — так и скажи одной строкой, не растягивай.

5. **Следующее пробуждение.** Используй `ScheduleWakeup` с интервалом ~2-3 минуты (вход по
   сигналу времязависим — детектор помечает сигнал устаревшим уже через ~15 минут, редкие
   проверки рискуют пропустить окно). Передай в `prompt` тот же литерал, которым был вызван этот
   цикл (per механика `/loop` dynamic mode), чтобы продолжение снова прочитало этот скилл.

## Ручной запрос пользователя

В любой момент между циклами пользователь может написать тебе прямо в эту беседу и попросить
проверить конкретную монету, которую он сам заметил, а детектор сигнал по ней ещё не дал (или
отфильтровал). Это НЕ часть обычного цикла по расписанию — обработай сразу, как только пришло
сообщение, не дожидаясь `ScheduleWakeup`:

1. **Бюджет.** Та же проверка, что в шаге 1 обычного цикла — если исчерпан, сообщи об этом и не
   трать вызов entry-agent (создать сигнал без последующей оценки смысла нет).
2. **Первичная проверка своими руками** (не спавни сабагента для этого шага — она бесплатна и не
   считается против бюджета). Вызови сам, напрямую:
   ```
   docker exec trading-bot python scripts/agent_data.py get_symbol_snapshot '{"symbol": "<SYM>/USDT:USDT", "bars": 30}'
   docker exec trading-bot python scripts/agent_data.py get_oi_trend '{"symbol": "<SYM>/USDT:USDT", "n_bars": 10}'
   ```
   Если по факту нет ни заметного роста цены, ни всплеска объёма (плоский график, обычный
   объём) — скажи пользователю прямо, что сетапа не видно, и на этом остановись, дальше не иди.
   Система long-only — если пользователь описывает шорт-сетап, сразу скажи, что режим этого не
   поддерживает, и не создавай сигнал.
3. **Создать сигнал.** Если первичные данные правдоподобны — создай строку в `signals` (в обход
   детектора):
   ```
   docker exec trading-bot python scripts/agent_actions.py create_manual_signal /tmp/decision.json
   ```
   где `decision.json` = `{"symbol": "<SYM>/USDT:USDT", "confidence": <твоя оценка 1-100>,
   "message": "<коротко, что заметил пользователь + что показала первичная проверка>"}`. Ответ
   содержит `signal_id`.
4. **Дальше — как шаг 2 обычного цикла**, но сразу, не дожидаясь следующего пробуждения:
   briefing → спавни `entry-agent` с промптом briefing + информация о сигнале (включая пометку,
   что это ручной запрос — сформируется автоматически, `create_manual_signal` сам добавляет её в
   `message`) → распарси вердикт → `open_entry` с этим `signal_id`, как обычно.
5. Расскажи пользователю результат (одобрено/отклонено и почему) и продолжай ждать следующего
   запланированного пробуждения как обычно — не нужно перепланировать `ScheduleWakeup` из-за
   этого внепланового шага, если до него ещё есть время.
6. **Если лимитник истёк неисполненным, а сетап всё ещё жив — решай сам, не спрашивай
   пользователя.** Проверь своими руками (`get_symbol_snapshot`/`get_oi_trend`), остыл ли сетап
   на самом деле, или монета просто двигалась быстрее отката лимитника (OI/объём продолжают
   расти, цена не развернулась). Если сетап жив — сразу создай новый `create_manual_signal` и
   повтори цикл (шаги 3-4), это НЕ требует разрешения пользователя каждый раз. Если после
   истёкшего лимитником захода сетап всё ещё силён, но лимитник снова не успевает исполниться
   до истечения — на следующей попытке прямо укажи entry-agent в промпте историю (сколько раз
   лимитник уже не исполнился и как менялся OI/объём) и попроси explicit рассмотреть
   `entry_mode: market`, а не снова спрашивать пользователя, что делать (найдено 24.07.2026 —
   монета ORDER, два неисполненных лимитника подряд на ускоряющемся OI, пользователь прямо
   указал: "Это должен решать ты, а не я", решение принимать сам). Останавливайся и спрашивай
   пользователя только если первичная проверка показывает реальное угасание сетапа (объём/OI
   больше не растут, цена развернулась) — тогда сообщи, что сетап, похоже, исчерпан, и не
   создавай новый сигнал.

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
