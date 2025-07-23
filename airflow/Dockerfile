# online-shopping-pipeline/airflow/Dockerfile
FROM apache/airflow:2.11.0-python3.11

USER root

# Java 17 설치
# Debian Bookworm (Airflow 2.8.1 이미지 기반)에서는 OpenJDK 17이 기본입니다.
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME 환경 변수 설정 (OpenJDK 17 경로에 맞게 수정)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# 🔽 Spark 설치 추가
ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

COPY requirements.txt .

ARG AIRFLOW_VERSION=2.11.0
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow[celery,postgres,redis]==${AIRFLOW_VERSION}" \
    -r requirements.txt
