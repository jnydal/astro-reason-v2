FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app
# copy and install base pins
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

# copy and install service-specific reqs
COPY service/worker_ingest/requirements/worker_ingest_requirements.txt ./requirements/
RUN pip install -r ./requirements/worker_ingest_requirements.txt -c requirements/constraints.txt

# now copy source
COPY . .
ENV PYTHONPATH=/app/service/worker_ingest/src:/app
CMD ["python", "-m", "jobs"]
