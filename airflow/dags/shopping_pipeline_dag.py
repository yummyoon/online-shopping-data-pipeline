# airflow/dags/shopping_pipeline_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG 정의: 파이프라인의 전체적인 흐름을 설정합니다.
with DAG(
    dag_id="online_shopping_pipeline", # Airflow UI에 표시될 DAG의 고유 ID
    start_date=pendulum.datetime(2025, 6, 15, tz="Asia/Seoul"), # 파이프라인이 언제부터 유효한지 설정
    schedule=None,    # None으로 설정하면 수동으로만 실행됩니다. (나중에 "@daily" 등으로 변경 가능)
    catchup=False,    # 시작일자부터 놓친 모든 스케줄을 한 번에 실행할지 여부. 보통 False로 둡니다.
    tags=["shopping-mall", "data-pipeline"], # UI에서 DAG를 쉽게 찾기 위한 태그
) as dag:
    
    # Task 1: 원본 로그 데이터 생성 작업
    # BashOperator를 사용하여 파이썬 스크립트를 실행합니다.
    # docker-compose.yaml에서 src 폴더를 /opt/airflow/src로 마운트했기 때문에 이 경로를 사용합니다.
    generate_raw_data = BashOperator(
        task_id="generate_raw_data", # 작업의 고유 ID
        bash_command="python /opt/airflow/src/data/raw_data_generator.py",
    )

    # Task 2: Spark로 데이터 처리 및 적재 작업
    # SparkSubmitOperator를 사용하여 Spark 작업을 제출합니다.
    submit_spark_job = SparkSubmitOperator(
        task_id="process_logs_with_spark",
        # ⭐️ Airflow UI에서 설정할 Connection의 ID
        conn_id="spark_default", 
        # 🟢 수정! Spark 애플리케이션의 실제 경로 (Airflow Worker 컨테이너 내부 마운트 경로)
        application="/opt/airflow/src/spark_jobs/process_raw_logs.py",
        # 🟢 수정! JDBC 드라이버 JAR 파일의 실제 경로 (Airflow Worker 컨테이너 내부 마운트 경로)
        jars="/opt/airflow/jars/postgresql-42.7.6.jar",
        name="arrow-spark",
        queue="root.default",
        # Spark 작업 실행에 필요한 환경 변수들
        # docker-compose에서 Spark 컨테이너에 설정한 변수와 동일하게 맞춰줍니다.
        env_vars={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_RAW_DATA_BUCKET": "raw-logs",
            "DW_POSTGRES_DB": "analytics_db",
            "DW_POSTGRES_USER": "pipeline_user",
            "DW_POSTGRES_PASSWORD": "pipeline_password",
        },
    )

    # 파이프라인 작업 순서 정의
    # generate_raw_data 작업이 성공적으로 끝나야 submit_spark_job 작업이 시작됩니다.
    generate_raw_data >> submit_spark_job
