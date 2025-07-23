# src/spark_jobs/process_raw_logs.py

import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

def create_spark_session(app_name):
    """
    SparkSession을 생성하고 PostgreSQL 접속 정보를 반환합니다.
    """
    # S3A 설정 - 환경 변수에서 가져오거나 기본값을 사용합니다.
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # PostgreSQL 접속 정보 - 환경 변수에서 가져오거나 기본값을 사용합니다.
    pg_host = os.getenv("PG_HOST", "postgres")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_database = os.getenv("PG_DATABASE", "airflow")
    pg_user = os.getenv("PG_USER", "airflow")
    pg_password = os.getenv("PG_PASSWORD", "airflow")
    
    pg_config = {
        "url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}",
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }
    
    return spark, pg_config

def process_and_load_data(spark: SparkSession, minio_bucket: str, execution_date: str, pg_config: dict, output_table: str):
    """
    지정된 날짜의 원본 로그 데이터를 읽어와 처리하고 PostgreSQL에 적재합니다.
    """
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
                          .withColumn("processing_date", lit(execution_date))

        # 필요한 컬럼만 선택
        processed_df = processed_df.select("user_id", "product_id", "price", "quantity", "action", "timestamp", "processing_date")

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
        process_and_load_data(spark, minio_raw_data_bucket, execution_date, pg_config, output_table)
        spark.stop()
        print("Spark application finished.")
    else:
        print("Failed to create SparkSession. Exiting.")
