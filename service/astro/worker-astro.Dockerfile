FROM base AS astro

# Add astro-only deps
COPY requirements/astro.txt requirements/constraints.txt ./requirements/
RUN pip install -r requirements/astro.txt -c requirements/constraints.txt

# Swiss Ephemeris data (recommended if you use pyswisseph)
# Choose one: download at build time OR COPY from repo if you vendor it.
# Example: download to /opt/ephe
RUN mkdir -p /opt/ephe && \
    curl -L -o /opt/ephe/sepl_18.se1 https://www.astro.com/ftp/swisseph/ephe/sepl_18.se1 && \
    true
ENV SE_EPHE_PATH=/opt/ephe

# default command (override in compose)
CMD ["python", "-m", "app.workers.astro_features"]
