FROM apache/airflow:2.9.3-python3.12

USER root

# Java 17 runs the Beam JAR; the symlink gives one JAVA_HOME on amd64 and arm64.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# PySpark is required for the large-file splitting phase.
RUN pip install --no-cache-dir pyspark==3.5.6
