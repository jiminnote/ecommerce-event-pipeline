-- ============================================================
-- 이커머스 행동 로그 파이프라인 - 테이블 정의
-- ============================================================
-- 초기화 시 1회 실행 (docker-entrypoint-initdb)
-- 마트 테이블은 DDL만 정의, 데이터는 Airflow DAG에서 증분 적재
-- ============================================================

-- ========== RAW LAYER (원천 데이터) ==========

CREATE TABLE IF NOT EXISTS raw_events (
    event_id        VARCHAR(36) PRIMARY KEY,
    event_type      VARCHAR(20) NOT NULL,
    user_id         VARCHAR(20) NOT NULL,
    session_id      VARCHAR(36) NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    platform        VARCHAR(10) DEFAULT 'web',
    page_url        VARCHAR(500),
    page_type       VARCHAR(30),
    element_id      VARCHAR(100),
    element_type    VARCHAR(30),
    product_id      VARCHAR(20),
    category_id     VARCHAR(20),
    quantity        INTEGER,
    unit_price      NUMERIC(12, 2),
    order_id        VARCHAR(30),
    total_amount    NUMERIC(12, 2),
    payment_method  VARCHAR(20),
    search_query    VARCHAR(200),
    result_count    INTEGER,
    referrer        VARCHAR(500),
    device_type     VARCHAR(20),
    os              VARCHAR(20),
    browser         VARCHAR(30),
    extra_data      JSONB,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp  ON raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id    ON raw_events(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_session_id ON raw_events(session_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_date       ON raw_events(DATE(timestamp));


-- ========== QUALITY CHECK LOG ==========

CREATE TABLE IF NOT EXISTS quality_check_log (
    check_id        SERIAL PRIMARY KEY,
    check_date      DATE NOT NULL,
    check_name      VARCHAR(100) NOT NULL,
    check_type      VARCHAR(50) NOT NULL,
    target_table    VARCHAR(100) NOT NULL,
    total_records   INTEGER,
    failed_records  INTEGER,
    pass_rate       NUMERIC(5, 2),
    status          VARCHAR(10) NOT NULL,   -- 'PASS' or 'FAIL'
    detail          TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ========== PIPELINE RUN LOG ==========

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          SERIAL PRIMARY KEY,
    run_date        DATE NOT NULL,
    dag_run_id      VARCHAR(200),
    total_events    INTEGER,
    unique_users    INTEGER,
    unique_sessions INTEGER,
    purchase_count  INTEGER,
    total_revenue   NUMERIC(14, 2),
    quality_status  VARCHAR(10),
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ========== MART LAYER (분석용 마트 DDL) ==========
-- 실제 데이터 적재는 sql/marts/*.sql 에서 증분(DELETE+INSERT) 방식으로 수행

CREATE TABLE IF NOT EXISTS mart_user_daily (
    user_id              VARCHAR(20)  NOT NULL,
    activity_date        DATE         NOT NULL,
    session_count        INTEGER      DEFAULT 0,
    page_view_count      INTEGER      DEFAULT 0,
    click_count          INTEGER      DEFAULT 0,
    add_to_cart_count    INTEGER      DEFAULT 0,
    purchase_count       INTEGER      DEFAULT 0,
    search_count         INTEGER      DEFAULT 0,
    total_purchase_amount NUMERIC(14, 2) DEFAULT 0,
    first_event_at       TIMESTAMP,
    last_event_at        TIMESTAMP,
    session_duration_total INTERVAL,
    PRIMARY KEY (user_id, activity_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_user_daily_date ON mart_user_daily(activity_date);


CREATE TABLE IF NOT EXISTS mart_funnel_daily (
    funnel_date          DATE        NOT NULL,
    platform             VARCHAR(10) NOT NULL,
    step1_viewers        INTEGER     DEFAULT 0,
    step2_clickers       INTEGER     DEFAULT 0,
    step3_cart_adders    INTEGER     DEFAULT 0,
    step4_purchasers     INTEGER     DEFAULT 0,
    view_to_click_rate   NUMERIC(6, 2),
    click_to_cart_rate   NUMERIC(6, 2),
    cart_to_purchase_rate NUMERIC(6, 2),
    overall_conversion_rate NUMERIC(6, 2),
    PRIMARY KEY (funnel_date, platform)
);

CREATE INDEX IF NOT EXISTS idx_mart_funnel_date ON mart_funnel_daily(funnel_date);


CREATE TABLE IF NOT EXISTS mart_product_daily (
    product_id           VARCHAR(20) NOT NULL,
    activity_date        DATE        NOT NULL,
    click_count          INTEGER     DEFAULT 0,
    cart_add_count       INTEGER     DEFAULT 0,
    purchase_count       INTEGER     DEFAULT 0,
    revenue              NUMERIC(14, 2) DEFAULT 0,
    units_sold           INTEGER     DEFAULT 0,
    unique_viewers       INTEGER     DEFAULT 0,
    unique_buyers        INTEGER     DEFAULT 0,
    conversion_rate      NUMERIC(6, 2),
    PRIMARY KEY (product_id, activity_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_product_date ON mart_product_daily(activity_date);
CREATE INDEX IF NOT EXISTS idx_mart_product_id   ON mart_product_daily(product_id);


CREATE TABLE IF NOT EXISTS mart_orders (
    order_id             VARCHAR(30) PRIMARY KEY,
    user_id              VARCHAR(20) NOT NULL,
    order_date           DATE        NOT NULL,
    order_timestamp      TIMESTAMP   NOT NULL,
    total_amount         NUMERIC(14, 2),
    payment_method       VARCHAR(20),
    platform             VARCHAR(10),
    discount_amount      NUMERIC(12, 2) DEFAULT 0,
    shipping_fee         NUMERIC(12, 2) DEFAULT 0,
    coupon_code          VARCHAR(30),
    item_count           INTEGER     DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_mart_orders_date ON mart_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_mart_orders_user ON mart_orders(user_id);
