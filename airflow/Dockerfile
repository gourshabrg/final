FROM apache/airflow:2.9.3-python3.12

USER root

# Java 17 is required to execute the Apache Beam JAR.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# PySpark is required for the large-file splitting phase.
RUN pip install --no-cache-dir pyspark==3.5.6
