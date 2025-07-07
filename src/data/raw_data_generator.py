# src/data/raw_data_generator.py

import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from minio import Minio
from minio.error import S3Error
import io

# --- 환경 변수 로드 ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_RAW_DATA_BUCKET = os.getenv("MINIO_RAW_DATA_BUCKET")
LOGS_PER_DAY = int(os.getenv("LOGS_PER_DAY", "100000"))

fake = Faker('ko_KR')

# ⭐️ 메인 로직을 Airflow가 호출할 수 있는 함수로 변경
def generate_log_entry(current_time: datetime):
    user_id = fake.uuid4()
    item_id = fake.random_int(min=1000, max=9999)
    price = round(random.uniform(5000, 100000), 2)
    quantity = random.randint(1, 5)
    event_type = random.choice(['view', 'add_to_cart', 'purchase', 'search'])
    search_query = fake.word() if event_type == 'search' else None

    log = {
        "timestamp": current_time.isoformat(),
        "user_id": user_id,
        "item_id": item_id,
        "event_type": event_type,
        "price": price if event_type in ['add_to_cart', 'purchase'] else None,
        "quantity": quantity if event_type in ['add_to_cart', 'purchase'] else None,
        "search_query": search_query,
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4_public(),
        "referrer": fake.url() if random.random() < 0.5 else None, # 50% 확률로 referrer 생성
        "campaign_source": random.choice([fake.word(), None]),
        "product_category": random.choice(['electronics', 'fashion', 'books', 'food', 'home', 'sports', 'beauty']),
        "is_bot": random.random() < 0.01 # 1% 확률로 봇 트래픽
    }

    # 간단한 전처리: Null 값 처리 및 타입 맞추기 (Spark에서 더 정교하게 처리)
    # Python에서 None은 JSON의 null로 변환됨

    return log

# --- MinIO 업로드 함수 ---
def upload_to_minio(data_lines: list, date_str: str, bucket_name: str):
    """
    생성된 로그 데이터를 MinIO에 업로드합니다.
    """
    # ⭐️ 1. bucket_name이 None인지 먼저 확인하여 에러 방지
    if not bucket_name:
        print("Error: Bucket name is not provided. Cannot upload to MinIO.")
        # ⭐️ 2. 에러가 발생했음을 Airflow에 알리기 위해 예외를 다시 발생시킴
        raise ValueError("Bucket name is None, upload failed.")

    try:
        # MinIO 클라이언트 초기화
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # 버킷이 없으면 생성
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        object_name = f"dt={date_str}/raw_logs.json"
        
        data_bytes = "\n".join(data_lines).encode('utf-8')
        data_length = len(data_bytes)

        client.put_object(
            bucket_name,
            object_name,
            io.BytesIO(data_bytes),
            data_length,
            content_type="application/json"
        )
        print(f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'.")

    except Exception as e:
        print(f"An unexpected error occurred during MinIO upload: {e}")
        # ⭐️ 2. 에러가 발생했음을 Airflow에 알리기 위해 예외를 다시 발생시킴
        raise e

# ⭐️ 메인 로직을 Airflow가 호출할 수 있는 함수로 변경
def generate_data_task(**context):
    """
    Airflow로부터 실행 날짜를 받아, 해당 날짜의 데이터를 생성하고 MinIO에 업로드합니다.
    """
    # 1. Airflow 컨텍스트에서 실행 날짜를 가져옵니다.
    execution_date_str = context["ds"]
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")

    # 2. 필요한 환경 변수를 로드합니다.
    MINIO_RAW_DATA_BUCKET = os.getenv("MINIO_RAW_DATA_BUCKET", "raw-logs")
    LOGS_PER_DAY = int(os.getenv("LOGS_PER_DAY", "100000"))

    print(f"--- Starting raw data generation for a single day: {execution_date_str} ---")
    print(f"Logs to generate: {LOGS_PER_DAY}")
    
    # 3. 하루치 데이터만 생성 (기존 로직과 동일)
    print(f"Generating logs for {execution_date_str}...")
    daily_logs = []
    for _ in range(LOGS_PER_DAY):
        random_seconds = random.randint(0, 86399)
        log_time = execution_date + timedelta(seconds=random_seconds)
        log_entry = generate_log_entry(log_time)
        daily_logs.append(json.dumps(log_entry, ensure_ascii=False))

    upload_to_minio(daily_logs, execution_date_str, MINIO_RAW_DATA_BUCKET)
    print(f"Finished generating and uploading logs for {execution_date_str}.")

    print("\n--- Raw data generation completed ---")