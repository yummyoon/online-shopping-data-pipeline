# 공식 Airflow 2.8.1 이미지를 기반으로 사용
FROM apache/airflow:2.8.1

# 루트 권한으로 명령 실행 (권한 없으면 apt-get 설치 실패)
USER root

# Java 17 설치 (OpenJDK)
# Java 17 헤드리스 버전 설치. --no-install-recommends는 불필요한 GUI 패키지를 제외하기 위해 사용
# Bookworm은 Airflow 2.8.1이 사용하는 Debian 버전이므로, 해당 패키지를 설치
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless=17.0.11+9-1 --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



# JAVA_HOME 환경변수 설정 (Spark가 이 변수 없으면 오류남)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Java 버전 확인 (디버깅용)
RUN java -version

# 다시 airflow 사용자로 전환 (보안상 안전하게 유지하기 위함)
USER airflow
