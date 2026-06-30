.PHONY: run run-signal test migrate-create migrate-up backtest-load backtest-run backtest-run-live docker-build docker-up docker-down docker-logs docker-rebuild clean

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

backtest-load:
	.venv/bin/python -m src.backtest.loader --days 7

backtest-load-month:
	.venv/bin/python -m src.backtest.loader --days 30

backtest-run:
	.venv/bin/python -m src.backtest.runner $(ARGS)

backtest-run-live:
	.venv/bin/python -m src.backtest.runner --db data/trading_bot.db $(ARGS)

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
