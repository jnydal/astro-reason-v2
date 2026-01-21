FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/api ./service/api
RUN gradle :service:api:installDist --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/service/api/build/install/api /app
EXPOSE 8000
ENTRYPOINT ["/app/bin/api"]
