# spark/Dockerfile
FROM openjdk:17

# Spark & Hadoop 버전 설정
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

# Spark 설치
RUN curl -O https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-*.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-*.tgz

# 환경변수 설정
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /app
COPY ./jobs /app/jobs
