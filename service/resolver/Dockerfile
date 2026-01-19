FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/resolver ./service/resolver
RUN gradle :service:resolver:build --no-daemon

FROM eclipse-temurin:17-jre-slim
WORKDIR /app
COPY --from=build /app/service/resolver/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
