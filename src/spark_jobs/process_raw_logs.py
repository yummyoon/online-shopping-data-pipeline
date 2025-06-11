# src/spark_jobs/process_raw_logs.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_date

def create_spark_session(app_name):
    """
    MinIO에 연결하기 위한 Hadoop 설정을 포함하여 SparkSession을 생성합니다.
    """
    # .env 파일에서 환경 변수를 직접 가져오기 (컨테이너 내에서 실행 시 Docker 환경 변수를 통해 주입됨)
    # 실제로는 docker-compose.yaml 또는 Spark submit 시 --conf 옵션으로 넘겨받는 것이 일반적입니다.
    # 여기서는 예시를 위해 SparkSession 설정에 직접 포함합니다.
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin") # .env 또는 Docker env에서 로드
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin") # .env 또는 Docker env에서 로드
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000") # MinIO 컨테이너의 내부 네트워크 주소

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark

def process_data(spark: SparkSession, minio_bucket: str):
    """
    MinIO에서 원본 로그 데이터를 읽어와 처리하고 결과를 출력합니다.
    """
    input_path = f"s3a://{minio_bucket}/logs/*/*.json" # MinIO의 특정 버킷 내 모든 로그 파일 지정
    output_path = f"s3a://{minio_bucket}/processed_logs/" # 처리된 데이터를 저장할 경로 (MinIO 내)

    print(f"Reading data from: {input_path}")
    
    try:
        # MinIO에서 JSON 파일 읽기 (자동으로 스키마 추론)
        df = spark.read.json(input_path)
        
        print("Schema of raw data:")
        df.printSchema()

        # 데이터 변환:
        # 1. timestamp 컬럼을 실제 Timestamp 타입으로 변환
        # 2. price, quantity가 null일 경우 0으로 대체 (예시)
        # 3. 처리일자 컬럼 추가
        processed_df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                         .withColumn("price", col("price").cast("double")) \
                         .withColumn("quantity", col("quantity").cast("integer")) \
                         .withColumn("price", col("price").cast("double").alias("price")) \
                         .withColumn("quantity", col("quantity").cast("integer").alias("quantity")) \
                         .withColumn("price", col("price").cast("double")) \
                         .withColumn("quantity", col("quantity").cast("integer")) \
                         .fillna(0, subset=['price', 'quantity']) \
                         .withColumn("processing_date", current_date())

        print("Schema of processed data:")
        processed_df.printSchema()

        print("Sample of processed data:")
        processed_df.show(5, truncate=False) # 5개 행 출력, 긴 문자열 잘리지 않게

        # 처리된 데이터를 MinIO의 다른 경로에 저장 (예: Parquet 형식)
        # 실제 파이프라인에서는 여기에 데이터 웨어하우스 적재 로직이 들어갑니다.
        # 편의상 MinIO에 저장하는 예시를 남깁니다.
        # processed_df.write.mode("overwrite").parquet(output_path)
        # print(f"Processed data written to: {output_path}")

    except Exception as e:
        print(f"Error processing data: {e}")
        # 오류 발생 시 스택 트레이스 출력
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    app_name = "MinIO_Log_Processor"
    minio_raw_data_bucket = os.getenv("MINIO_RAW_DATA_BUCKET", "raw-logs")

    spark = create_spark_session(app_name)
    
    if spark:
        print(f"SparkSession created successfully for app: {app_name}")
        process_data(spark, minio_raw_data_bucket)
        spark.stop()
        print("Spark application finished.")
    else:
        print("Failed to create SparkSession. Exiting.")