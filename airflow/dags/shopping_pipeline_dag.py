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
        # ✅ 2. Spark 워커가 접근 가능한 경로로 수정
        application="/opt/bitnami/spark/jobs/process_raw_logs.py",
        # ✅ 1. 데이터 핸드오프를 위해 실행 날짜 전달
        application_args=["--date", "{{ ds }}"],
        jars="/opt/bitnami/spark/jars/postgresql-42.7.6.jar",
        name="arrow-spark",
        queue="root.default",
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

    generate_raw_data >> submit_spark_job