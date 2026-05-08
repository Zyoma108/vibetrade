.PHONY: run run-signal run-virtual test migrate-create migrate-up clean

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

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
