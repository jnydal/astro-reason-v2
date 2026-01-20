FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle.kts settings.gradle.kts ./
COPY service/core ./service/core
COPY service/traits ./service/traits
RUN gradle :service:traits:build --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/service/traits/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
