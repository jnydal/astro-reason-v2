FROM base AS ingest
COPY requirements/ingest.txt requirements/constraints.txt ./requirements/
RUN pip install -r requirements/ingest.txt -c requirements/constraints.txt
CMD ["python", "-m", "app.workers.ingest"]
