# docker/test.Dockerfile
FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && rm -rf /var/lib/apt/lists/*

# --- deps (copy from BOTH locations) ---
COPY requirements/base.txt /app/requirements/base.txt
COPY docker/requirements_test.txt /app/requirements/test.txt

RUN pip install --upgrade pip \
 && pip install -r /app/requirements/base.txt \
 && pip install -r /app/requirements/test.txt

# --- app code ---
COPY . /app
# Editable install only if project has metadata
RUN if [ -f pyproject.toml ] || [ -f setup.cfg ] || [ -f setup.py ]; then pip install -e .; fi

CMD ["pytest", "-q"]
