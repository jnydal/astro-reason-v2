FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

COPY service/worker_astro_interpret/requirements/worker_astro_interpret_requirements.txt ./requirements/
RUN pip install -r ./requirements/worker_astro_interpret_requirements.txt -c requirements/constraints.txt

COPY . .
ENV PYTHONPATH=/app

CMD ["python", "-m", "service.worker_astro_interpret.app.interpret_worker"]
