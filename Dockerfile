FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

COPY migrations/ migrations/
COPY alembic.ini .
RUN mkdir -p /app/data

CMD ["python", "-m", "src.main", "--config", "config/config.yaml"]
