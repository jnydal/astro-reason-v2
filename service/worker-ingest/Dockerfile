FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/worker-ingest ./service/worker-ingest
RUN gradle :service:worker-ingest:installDist --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/service/worker-ingest/build/install/worker-ingest /app
ENTRYPOINT ["/app/bin/worker-ingest"]
