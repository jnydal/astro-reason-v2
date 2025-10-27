FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# Make sure /app is on sys.path for imports like `import jobs`
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir httpx pydantic

COPY jobs ./jobs
COPY app ./app

# Run as a module to get proper package import semantics
CMD ["python", "-m", "app.worker_main"]
