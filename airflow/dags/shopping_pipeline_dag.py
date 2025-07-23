# airflow/dags/shopping_pipeline_dag.py

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from data.raw_data_generator import generate_data_task

with DAG(
    dag_id="online_shopping_pipeline",
    start_date=pendulum.datetime(2025, 7, 7, tz="Asia/Seoul"),
    schedule="@daily", # ⭐️ 매일 실행되도록 스케줄 설정
    catchup=False,
    tags=["shopping-mall", "data-pipeline"],
) as dag:
    
    generate_raw_data = PythonOperator(
        task_id="generate_raw_data",
        # ✅ 1. 데이터 핸드오프를 위해 실행 날짜 전달
        python_callable=generate_data_task,
    )

    submit_spark_job = SparkSubmitOperator(
        task_id="process_logs_with_spark",
        conn_id="spark_default",
        application="/opt/airflow/src/spark_jobs/process_raw_logs.py",
        application_args=["--date", "{{ ds }}"],
        # ✅ 1. 필요한 JAR 파일들을 쉼표로 구분하여 모두 지정
        # ✅ 1. Airflow Worker 컨테이너 기준으로 postgresql-42.7.6.jar 경로 수정
        jars="/opt/airflow/jars/postgresql-42.7.6.jar",
        name="arrow-spark",
        env_vars={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_RAW_DATA_BUCKET": "raw-logs",
            "DW_POSTGRES_DB": "analytics_db",
            "DW_POSTGRES_USER": "pipeline_user",
            "DW_POSTGRES_PASSWORD": "pipeline_password",
        },
        # ✅ 2. Spark가 MinIO를 사용하도록 상세 설정 추가
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )

    generate_raw_data >> submit_spark_job