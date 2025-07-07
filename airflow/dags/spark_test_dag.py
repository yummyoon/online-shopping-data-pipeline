# airflow/dags/spark_test_dag.py
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_connection_test_dag",
    start_date=pendulum.datetime(2025, 7, 7, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
    tags=["test", "spark"],
) as dag:
    test_spark_pi = SparkSubmitOperator(
        task_id="test_spark_pi",
        conn_id="spark_default",  # Airflow UI에 설정된 Connection ID
        # Spark에 내장된 예제 JAR 파일 경로
        application="/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
        # 실행할 예제 클래스
        java_class="org.apache.spark.examples.SparkPi",
        # 예제에 전달할 인자 (계산할 파이의 자릿수)
        application_args=["10"],
        name="spark-pi-test",
        queue="root.default",
    )
