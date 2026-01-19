FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/api ./service/api
RUN gradle :service:api:build --no-daemon

FROM eclipse-temurin:17-jre-slim
WORKDIR /app
COPY --from=build /app/service/api/build/libs/*.jar app.jar
EXPOSE 8000
ENTRYPOINT ["java", "-jar", "app.jar"]
