FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

FROM flink:1.20.0-java17
RUN mkdir -p /opt/flink/usrlib /opt/flink/checkpoints \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints
COPY --from=build --chown=flink:flink /src/target/flink-checkpointer.jar /opt/flink/usrlib/flink-checkpointer.jar
