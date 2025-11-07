FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# system deps for building wheels if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy minimal files first for better caching
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

# app code (kept last to leverage Docker layer cache)
COPY app/ ./app/
