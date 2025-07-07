# src/spark_jobs/process_raw_logs.py

import os
import sys  # ⭐️ 1. sys 라이브러리 추가
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_date
from pyspark.sql.types import DoubleType, IntegerType

def create_spark_session(app_name):
    # ... (이 함수는 수정할 필요 없습니다. 그대로 사용합니다.)
    # ...
    # ⭐️ 3. DB 호스트 이름을 docker-compose.yml의 서비스 이름으로 수정
    pg_host = "postgres"  # 또는 docker-compose.yml에 정의된 서비스 이름
    # ...
    # ... (나머지 pg_config 설정) ...
    return spark, pg_config

def process_and_load_data(spark: SparkSession, minio_bucket: str, execution_date: str, pg_config: dict, output_table: str):
    """
    지정된 날짜의 원본 로그 데이터를 읽어와 처리하고 PostgreSQL에 적재합니다.
    """
    # ⭐️ 2. 와일드카드(*) 대신, 전달받은 날짜로 정확한 경로 지정
    input_path = f"s3a://{minio_bucket}/dt={execution_date}/"
    
    print(f"Reading data from: {input_path}")
    
    try:
        df = spark.read.json(input_path)
        
        print("Schema of raw data:")
        df.printSchema()

        # 데이터 변환
        processed_df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) \
                          .withColumn("price", col("price").cast(DoubleType())) \
                          .withColumn("quantity", col("quantity").cast(IntegerType())) \
                          .fillna(0, subset=['price', 'quantity']) \
                          .withColumn("processing_date", lit(execution_date)) # lit() 함수로 execution_date 사용
                          # ... (select 등 나머지 변환 로직)

        print("Schema of processed data (for DB):")
        processed_df.printSchema()
        processed_df.show(5, truncate=False)

        # PostgreSQL에 데이터 적재
        print(f"Loading data into PostgreSQL table: {output_table}")
        processed_df.write \
            .format("jdbc") \
            .options(**pg_config) \
            .option("dbtable", output_table) \
            .mode("append") \
            .save()
        print(f"Successfully loaded data into PostgreSQL table '{output_table}'.")
    except Exception as e:
        print(f"Error processing and loading data for date {execution_date}: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # ⭐️ 1. Airflow로부터 --date 인자를 받음
    if len(sys.argv) < 3 or sys.argv[1] != '--date':
        print("Usage: spark-submit process_raw_logs.py --date YYYY-MM-DD")
        sys.exit(1)
    
    execution_date = sys.argv[2]

    app_name = f"MinIO_Log_Processor_{execution_date}"
    minio_raw_data_bucket = os.getenv("MINIO_RAW_DATA_BUCKET", "raw-logs")
    output_table = "processed_logs"

    spark, pg_config = create_spark_session(app_name)

    if spark:
        print(f"SparkSession created successfully for app: {app_name}")
        # ⭐️ 1. 전달받은 날짜를 process_and_load_data 함수에 넘겨줌
        process_and_load_data(spark, minio_raw_data_bucket, execution_date, pg_config, output_table)
        spark.stop()
        print("Spark application finished.")
    else:
        print("Failed to create SparkSession. Exiting.")