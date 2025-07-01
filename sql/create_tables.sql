-- sql/create_tables.sql

-- 기존 테이블이 있다면 삭제 (개발 초기 단계에서 유용)
DROP TABLE IF EXISTS processed_logs;

-- 처리된 로그 데이터를 저장할 테이블 생성
CREATE TABLE processed_logs (
    log_id SERIAL PRIMARY KEY,
    "timestamp" TIMESTAMP,
    user_id VARCHAR(255),
    item_id INTEGER,
    event_type VARCHAR(50),
    price DOUBLE PRECISION,
    quantity INTEGER,
    search_query TEXT,
    user_agent TEXT,
    ip_address VARCHAR(50),
    referrer TEXT,
    campaign_source VARCHAR(255),
    product_category VARCHAR(100),
    is_bot BOOLEAN,
    processing_date DATE
);

-- 자주 조회될 컬럼에 인덱스 생성 (성능 향상)
CREATE INDEX idx_timestamp ON processed_logs("timestamp");
CREATE INDEX idx_event_type ON processed_logs(event_type);
CREATE INDEX idx_product_category ON processed_logs(product_category);

-- 테이블 생성 확인
COMMENT ON TABLE processed_logs IS 'Spark에서 처리된 원본 로그 데이터가 저장되는 테이블';