FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
# copy and install base pins
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

# copy and install service-specific reqs (correct path)
COPY service/ingest/requirements/ingest_requirements.txt ./requirements/
RUN pip install -r ./requirements/ingest_requirements.txt -c requirements/constraints.txt

# Install FastAPI and uvicorn for the API
RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic

# now copy source
COPY . .
ENV PYTHONPATH=/app/service:/app

EXPOSE 8002
CMD ["python", "-m", "service.ingest.app.fetch_bio_api"]
