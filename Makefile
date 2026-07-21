.PHONY: run run-signal test migrate-create migrate-up backtest-run backtest-run-live docker-build docker-up docker-down docker-logs docker-rebuild clean

APP := .venv/bin/python -m src.main

run:
	$(APP) --config config/config.yaml

run-signal:
	$(APP) --config config/config.yaml --mode signal

test:
	.venv/bin/pytest -v

migrate-create:
	.venv/bin/alembic revision --autogenerate -m "$(name)"

migrate-up:
	.venv/bin/alembic upgrade head

backtest-run:
	.venv/bin/python -m src.backtest.runner $(ARGS)

backtest-run-live:
	# Живая БД теперь в named Docker volume (не на хосте) — копируем снапшот перед прогоном
	docker cp trading-bot:/app/data/trading_bot.db data/trading_bot.live-snapshot.db
	.venv/bin/python -m src.backtest.runner --db data/trading_bot.live-snapshot.db $(ARGS)

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-rebuild:
	docker compose down
	docker compose build --no-cache
	docker compose up -d
	docker compose logs -f

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
