import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from minio import Minio
from minio.error import S3Error

# --- 환경 변수 로드 (로컬 테스트용) ---
# 실제 배포 시에는 .env 파일에서 자동으로 로드됩니다.
# 로컬에서 스크립트 단독 실행 시를 위해 기본값 설정
load_dotenv_success = False
try:
    from dotenv import load_dotenv
    if load_dotenv(): # .env 파일이 있으면 로드
        load_dotenv_success = True
except ImportError:
    print("python-dotenv library not found. Please install with 'pip install python-dotenv'.")

if not load_dotenv_success:
    print("Warning: .env file not loaded. Using default environment variables for local testing.")
    os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
    os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin") # MinIO docker-compose.yaml 에 설정할 기본값
    os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin") # MinIO docker-compose.yaml 에 설정할 기본값
    os.environ.setdefault("MINIO_RAW_DATA_BUCKET", "raw-logs")
    os.environ.setdefault("NUM_DAYS_TO_GENERATE", "1") # 기본 1일치만 생성
    os.environ.setdefault("LOGS_PER_DAY", "100000") # 기본 10만 건

# --- 환경 변수 로드 ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_RAW_DATA_BUCKET = os.getenv("MINIO_RAW_DATA_BUCKET")

NUM_DAYS_TO_GENERATE = int(os.getenv("NUM_DAYS_TO_GENERATE", "10")) # 기본 10일치
LOGS_PER_DAY = int(os.getenv("LOGS_PER_DAY", "100000")) # 기본 10만 건/일

fake = Faker('ko_KR') # 한국어 로케일 설정

# --- 로그 엔트리 생성 함수 ---
def generate_log_entry(current_time: datetime):
    user_id = fake.uuid4()
    item_id = fake.random_int(min=1000, max=9999)
    price = round(random.uniform(5000, 100000), 2)
    quantity = random.randint(1, 5)
    event_type = random.choice(['view', 'add_to_cart', 'purchase', 'search'])
    search_query = fake.word() if event_type == 'search' else None
    
    log = {
        "timestamp": current_time.isoformat(), # ISO 8601 형식
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
    try:
        # MinIO 클라이언트 초기화
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False # 개발 환경에서는 보통 False, 프로덕션에서는 True (HTTPS 사용 시)
        )

        # 버킷이 없으면 생성
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # 데이터 업로드 (메모리 상에서 바로 업로드)
        # 파일 경로: logs/YYYY-MM-DD/YYYY-MM-DD_logs.json
        object_name = f"logs/{date_str}/{date_str}_logs.json"
        
        # 각 JSON 객체를 새 줄로 구분하여 하나의 큰 문자열로 만듦 (Newline-delimited JSON, NDJSON 형식)
        data_bytes = "\n".join(data_lines).encode('utf-8')
        data_length = len(data_bytes)

        client.put_object(
            bucket_name,
            object_name,
            data_bytes,
            data_length,
            content_type="application/json"
        )
        print(f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"MinIO S3 Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during MinIO upload: {e}")

# --- 메인 실행 로직 ---
if __name__ == "__main__":
    print(f"--- Starting raw data generation for {NUM_DAYS_TO_GENERATE} days ---")
    print(f"Logs per day: {LOGS_PER_DAY}")
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}, Bucket: {MINIO_RAW_DATA_BUCKET}")

    start_date = datetime.now() - timedelta(days=NUM_DAYS_TO_GENERATE) # N일 전부터 시작
    
    for i in range(NUM_DAYS_TO_GENERATE):
        current_date = start_date + timedelta(days=i)
        current_date_str = current_date.strftime("%Y-%m-%d")
        
        print(f"\nGenerating logs for {current_date_str}...")
        daily_logs = []
        for _ in range(LOGS_PER_DAY):
            log_entry = generate_log_entry(current_date)
            daily_logs.append(json.dumps(log_entry, ensure_ascii=False)) # JSON 문자열로 변환

        # 생성된 로그를 MinIO에 업로드
        upload_to_minio(daily_logs, current_date_str, MINIO_RAW_DATA_BUCKET)
        print(f"Finished generating and uploading logs for {current_date_str}.")

    print("\n--- Raw data generation completed ---")