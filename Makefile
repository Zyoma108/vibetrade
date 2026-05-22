.PHONY: run run-signal run-virtual test migrate-create migrate-up backtest-load backtest-run docker-build docker-up docker-down clean

APP := .venv/bin/python -m src.main

run:
	$(APP) --config config/config.yaml

run-signal:
	$(APP) --config config/config.yaml --mode signal

run-virtual:
	$(APP) --config config/config.yaml --mode virtual

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
