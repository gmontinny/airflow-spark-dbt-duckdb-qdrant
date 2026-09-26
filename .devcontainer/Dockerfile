FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/root/.local/bin:${PATH}" \
    UV_LINK_MODE=copy

RUN --mount=type=cache,target=/var/cache/apt \
    set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl make bash git ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /workspace

COPY pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
      pyspark>=4.0.0 \
      duckdb>=1.0.0 \
      dbt-duckdb>=1.8.0 \
      pyarrow>=10.0.0 \
      pandas>=2.0.0 \
      grpcio>=1.74.0 \
      grpcio-status>=1.74.0 \
      python-dotenv>=1.1.1 \
      apache-airflow-providers-fab

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
