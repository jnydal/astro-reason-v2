FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/resolver ./service/resolver
RUN gradle :service:resolver:installDist --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/service/resolver/build/install/resolver /app
ENTRYPOINT ["/app/bin/resolver"]
