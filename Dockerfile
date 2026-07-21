FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем всё и устанавливаем
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e ".[dev]"

COPY migrations/ migrations/
COPY alembic.ini .
COPY scripts/ scripts/

RUN mkdir -p /app/data

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf https://api.bybit.com/v5/market/time || exit 1

STOPSIGNAL SIGTERM

CMD ["python", "-m", "src.main", "--config", "/app/config/config.yaml"]
