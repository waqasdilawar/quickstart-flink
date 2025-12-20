
FROM flink:2.1.0-scala_2.12-java11
LABEL authors="waqas dilawar"

COPY target/quickstart-0.1.jar usrlib/

# Enable built-in S3 filesystem plugin from /opt/flink/opt (per Flink docs)
ENV ENABLE_BUILT_IN_PLUGINS="flink-s3-fs-hadoop-2.1.0.jar"

USER flink



#FROM maven:3-eclipse-temurin-11 AS build
#
#COPY pom.xml .
#RUN mvn dependency:go-offline
#
#COPY . .
#RUN mvn package
#
#FROM flink:2.1.0-scala_2.12-java11
#
#COPY --from=build --chown=flink:flink target/quickstart-0.1.jar /opt/flink-jobs/quickstart-0.1.jar
