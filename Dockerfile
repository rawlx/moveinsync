# Build stage: Maven + JDK 21 (pinned Maven patch).
FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY gateway/pom.xml gateway/pom.xml
RUN mvn -B -ntp -q -pl gateway -am dependency:go-offline
COPY gateway/src gateway/src
RUN mvn -B -ntp -q -pl gateway -am package -DskipTests

# Runtime stage: JRE 21 (tag floats on the 21.0.x patch; see ADR-0008).
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /src/gateway/target/gateway.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
