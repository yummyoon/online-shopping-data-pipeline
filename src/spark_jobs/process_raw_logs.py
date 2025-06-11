# src/spark_jobs/process_raw_logs.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_date
from pyspark.sql.types import DoubleType, IntegerType # 타입 임포트

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

    # PostgreSQL 접속 정보는 환경 변수에서 가져옵니다.
    # Docker Compose에서 Spark 컨테이너의 환경 변수로 설정해 줍니다.
    pg_db = os.getenv("POSTGRES_DB", "analytics_db")
    pg_user = os.getenv("POSTGRES_USER", "airflow")
    pg_password = os.getenv("POSTGRES_PASSWORD", "airflow")
    # Docker Compose 네트워크 내에서 PostgreSQL 컨테이너의 서비스 이름은 'db'입니다.
    pg_host = "db" 
    pg_port = "5432"

    # JDBC 드라이버 JAR 파일 경로 (Docker 컨테이너 내에서 Spark-submit 시 --jars 옵션으로 지정)
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # PostgreSQL 접속 정보 딕셔너리 반환
    pg_config = {
        "url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}",
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }
    
    return spark, pg_config

def process_and_load_data(spark: SparkSession, minio_bucket: str, pg_config: dict, output_table: str):
    """
    MinIO에서 원본 로그 데이터를 읽어와 처리하고 PostgreSQL에 적재합니다.
    """
    input_path = f"s3a://{minio_bucket}/logs/*/*.json"
    
    print(f"Reading data from: {input_path}")
    
    try:
        df = spark.read.json(input_path)
        
        print("Schema of raw data:")
        df.printSchema()

        # 데이터 변환 (이전과 동일)
        processed_df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) \
                         .withColumn("price", col("price").cast(DoubleType())) \
                         .withColumn("quantity", col("quantity").cast(IntegerType())) \
                         .fillna(0, subset=['price', 'quantity']) \
                         .withColumn("processing_date", current_date()) \
                         .select( # 필요한 컬럼만 선택하고 순서 조정 (테이블 스키마 고려)
                             "timestamp", "user_id", "item_id", "event_type", 
                             "price", "quantity", "search_query", "user_agent", 
                             "ip_address", "referrer", "campaign_source", 
                             "product_category", "is_bot", "processing_date"
                         )

        print("Schema of processed data (for DB):")
        processed_df.printSchema()
        print("Sample of processed data (for DB):")
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
        print(f"Error processing and loading data: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    app_name = "MinIO_Log_Processor"
    minio_raw_data_bucket = os.getenv("MINIO_RAW_DATA_BUCKET", "raw-logs")
    output_table = "processed_logs" # PostgreSQL에 저장될 테이블 이름

    # SparkSession과 PostgreSQL 접속 정보 가져오기
    spark, pg_config = create_spark_session(app_name)

    if spark:
        print(f"SparkSession created successfully for app: {app_name}")
        process_and_load_data(spark, minio_raw_data_bucket, pg_config, output_table)
        spark.stop()
        print("Spark application finished.")
    else:
        print("Failed to create SparkSession. Exiting.")