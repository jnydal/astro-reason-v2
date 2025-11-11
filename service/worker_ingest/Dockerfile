FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app
# copy and install base pins
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

# copy and install service-specific reqs (correct path)
COPY service/astro/requirements/worker_embeddings_requirements.txt ./requirements/
RUN pip install -r ./requirements/worker_embeddings_requirements.txt -c requirements/constraints.txt

# now copy source
COPY . .
ENV PYTHONPATH=/app/src:/app
CMD ["rq", "worker", "-u", "redis://redis:6379/0", "ingest"]
