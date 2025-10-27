FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx pydantic
COPY jobs ./jobs
COPY app ./app
CMD ["python", "app/worker_main.py"]
