FROM base AS resolver
COPY requirements/resolver.txt requirements/constraints.txt ./requirements/
RUN pip install -r requirements/resolver.txt -c requirements/constraints.txt
CMD ["python", "-m", "app.workers.resolver"]

