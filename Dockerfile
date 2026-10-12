# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LAKEHOUSE_ROOT=/data \
    DAGSTER_HOME=/data/.dagster

WORKDIR /app

# Dependencias primero: la capa se reutiliza mientras no cambie pyproject.toml.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[dashboard,orchestration]"

COPY config ./config
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit

RUN useradd --create-home --uid 1000 lakehouse \
    && mkdir -p /data/.dagster \
    && chown -R lakehouse:lakehouse /data /app
USER lakehouse

VOLUME ["/data"]
CMD ["lakehouse", "--help"]
