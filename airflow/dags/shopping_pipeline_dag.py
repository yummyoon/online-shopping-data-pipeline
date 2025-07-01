# airflow/dags/shopping_pipeline_dag.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="online_shopping_pipeline",
    start_date=pendulum.datetime(2025, 6, 15, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["shopping-mall", "data-pipeline"],
) as dag:
    
    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        # 💡 AIRFLOW_PROJ_DIR로 인해 src가 /opt/airflow/src에 마운트됨
        bash_command="python /opt/airflow/src/data/raw_data_generator.py",
    )

    submit_spark_job = SparkSubmitOperator(
        task_id="process_logs_with_spark",
        conn_id="spark_default", # Connection ID를 통해 Spark Master 정보를 가져옵니다.
        # 🟢 최종 수정: Spark Master/Worker 컨테이너 내부의 마운트 경로 (Bitnami 기본 경로 + 마운트된 src/spark_jobs)
        application="/opt/bitnami/spark/jobs/process_raw_logs.py",
        # 🟢 최종 수정: JDBC JAR 파일은 Airflow Worker 컨테이너에 마운트된 경로에서 Spark 클러스터로 전송
        jars="/opt/airflow/jars/postgresql-42.7.7.jar", # 💡 최신 버전으로 변경 (jars 폴더에 42.7.7이 있다면)
        name="arrow-spark",
        queue="root.default",
        env_vars={ # Spark 애플리케이션에 전달될 환경 변수
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_RAW_DATA_BUCKET": "raw-logs",
            "DW_POSTGRES_DB": "datawarehouse", # 💡 .env 파일과 일치
            "DW_POSTGRES_USER": "dwuser",     # 💡 .env 파일과 일치
            "DW_POSTGRES_PASSWORD": "dwpassword", # 💡 .env 파일과 일치
        },
    )

    # 🟢 데이터 웨어하우스 적재 태스크 (BashOperator 사용)
    load_to_warehouse = BashOperator(
        task_id="load_to_warehouse",
        bash_command="python /opt/airflow/src/storing/load_to_warehouse.py",
        env={ # 필요한 환경 변수를 명시적으로 전달
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_PROCESSED_DATA_BUCKET": "processed-logs", # Spark 처리 결과가 저장될 버킷 이름 (예시)
            "DW_POSTGRES_DB": "datawarehouse",
            "DW_POSTGRES_USER": "dwuser",
            "DW_POSTGRES_PASSWORD": "dwpassword",
        }
    )

    generate_raw_data >> submit_spark_job >> load_to_warehouse
