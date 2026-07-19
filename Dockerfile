# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM maven:3-eclipse-temurin-26 AS build
WORKDIR /app

COPY pom.xml ./
COPY src/ src/

# Build with a cached Maven repo (persists between builds)
RUN --mount=type=cache,target=/root/.m2 mvn -B clean package -DskipTests

# ---- Runtime stage ----
FROM eclipse-temurin:26-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
