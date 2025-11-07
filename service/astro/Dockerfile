FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
# copy and install base pins
COPY requirements/base.txt requirements/constraints.txt ./requirements/
RUN pip install --upgrade pip && \
    pip install -r requirements/base.txt -c requirements/constraints.txt

# copy and install service-specific reqs (correct path)
COPY service/astro/requirements/astro_requirements.txt ./requirements/
RUN pip install -r ./requirements/astro_requirements.txt -c requirements/constraints.txt

# Swiss Ephemeris data (optional but recommended)
RUN mkdir -p /opt/ephe && \
    curl -L -o /opt/ephe/sepl_18.se1 https://www.astro.com/ftp/swisseph/ephe/sepl_18.se1 || true
ENV SE_EPHE_PATH=/opt/ephe

# now copy source
COPY . .
ENV PYTHONPATH=/app/service:/app
CMD ["python", "-m", "service.astro.app.astro_features"]
