FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# service-specific requirements (relative to this folder)
COPY requirements/worker_embeddings_requirements.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r ./requirements/worker_embeddings_requirements.txt

# copy code
COPY . .

# make src importable -> "jobs.embed_person_bios" works
ENV PYTHONPATH=/app/src:/app

# Kafka consumer for the "embeddings" topic
CMD ["python", "-m", "jobs"]
