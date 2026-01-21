FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/astro ./service/astro
RUN gradle :service:astro:installDist --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
# Swiss Ephemeris data (optional but recommended)
RUN mkdir -p /opt/ephe && \
    apt-get update && \
    apt-get install -y curl && \
    curl -L -o /opt/ephe/sepl_18.se1 https://www.astro.com/ftp/swisseph/ephe/sepl_18.se1 || true && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV SE_EPHE_PATH=/opt/ephe
COPY --from=build /app/service/astro/build/install/astro /app
ENTRYPOINT ["/app/bin/astro"]
