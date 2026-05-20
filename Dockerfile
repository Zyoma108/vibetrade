FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости — отдельным слоем для кеширования
COPY pyproject.toml .
RUN pip install --no-cache-dir -e . && pip install --no-cache-dir -e ".[dev]"

# Код
COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini .

RUN mkdir -p /app/data

# ByBit testnet: https://api-testnet.bybit.com
# ByBit mainnet: https://api.bybit.com
HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -sf https://api.bybit.com/v5/market/time || exit 1

# Graceful shutdown через SIGTERM
STOPSIGNAL SIGTERM

CMD ["python", "-m", "src.main", "--config", "/app/config/config.yaml"]
