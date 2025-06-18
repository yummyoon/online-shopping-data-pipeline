# online-shopping-pipeline/airflow/Dockerfile
FROM apache/airflow:2.8.1

USER root

# Java 17 설치
# Debian Bookworm (Airflow 2.8.1 이미지 기반)에서는 OpenJDK 17이 기본입니다.
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME 환경 변수 설정 (OpenJDK 17 경로에 맞게 수정)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow
