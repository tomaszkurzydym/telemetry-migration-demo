# Packaging stub (SYNTHETIC DEMO): runs the legacy job with sbt (Scala 2.12.18, Spark 3.5.7, JDK 17).
FROM eclipse-temurin:17.0.13_11-jdk-jammy
ARG SBT_VERSION=1.10.7
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar -xz -C /opt
ENV PATH="/opt/sbt/bin:${PATH}"
WORKDIR /app
COPY build.sbt ./
COPY project/build.properties project/
RUN sbt -batch update
COPY src src
RUN sbt -batch compile
ENV TRIGGER=processing-time
ENTRYPOINT ["sbt", "-batch", "runMain demo.legacy.LegacyJob"]
