-- ============================================================
-- 데이터 마트 참조 문서 (전체 마트 스키마 한눈에 보기)
-- ============================================================
-- 주의: 이 파일은 마트 구조 참조용입니다.
-- 실제 운영:
--   DDL(테이블 생성) → sql/create_tables.sql
--   증분 적재        → sql/marts/mart_*.sql (Airflow DAG에서 실행)
--
-- 증분 적재 패턴: DELETE WHERE date = '{{ ds }}' → INSERT
-- 이 패턴은 멱등성(idempotency)을 보장하여 재실행 시 중복 없이 동작합니다.
-- ============================================================

-- ========== 사용자 마트 (User Mart) ==========
-- 사용자별 행동 요약 테이블

DROP TABLE IF EXISTS mart_user_daily;
CREATE TABLE mart_user_daily AS
SELECT
    user_id,
    DATE(timestamp) AS activity_date,
    COUNT(DISTINCT session_id) AS session_count,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN total_amount END), 0) AS total_purchase_amount,
    MIN(timestamp) AS first_event_at,
    MAX(timestamp) AS last_event_at,
    MAX(timestamp) - MIN(timestamp) AS session_duration_total
FROM raw_events
GROUP BY user_id, DATE(timestamp);

CREATE INDEX idx_mart_user_daily_date ON mart_user_daily(activity_date);
CREATE INDEX idx_mart_user_daily_user ON mart_user_daily(user_id);


-- ========== 전환 퍼널 마트 (Funnel Mart) ==========
-- 일별 전환 퍼널 집계

DROP TABLE IF EXISTS mart_funnel_daily;
CREATE TABLE mart_funnel_daily AS
SELECT
    DATE(timestamp) AS funnel_date,
    platform,
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) AS step1_viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END) AS step2_clickers,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS step3_cart_adders,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS step4_purchasers,
    -- 전환율 계산
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0) * 100, 2
    ) AS view_to_click_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END), 0) * 100, 2
    ) AS click_to_cart_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END), 0) * 100, 2
    ) AS cart_to_purchase_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0) * 100, 2
    ) AS overall_conversion_rate
FROM raw_events
GROUP BY DATE(timestamp), platform;

CREATE INDEX idx_mart_funnel_date ON mart_funnel_daily(funnel_date);


-- ========== 상품 마트 (Product Mart) ==========
-- 상품별 행동 집계

DROP TABLE IF EXISTS mart_product_daily;
CREATE TABLE mart_product_daily AS
SELECT
    product_id,
    DATE(timestamp) AS activity_date,
    COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_add_count,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN total_amount END), 0) AS revenue,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN quantity END), 0) AS units_sold,
    COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END) AS unique_viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_buyers,
    -- 상품별 전환율
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END), 0) * 100, 2
    ) AS conversion_rate
FROM raw_events
WHERE product_id IS NOT NULL
GROUP BY product_id, DATE(timestamp);

CREATE INDEX idx_mart_product_date ON mart_product_daily(activity_date);
CREATE INDEX idx_mart_product_id ON mart_product_daily(product_id);


-- ========== 주문 마트 (Order Mart) ==========
-- 주문 단위 집계

DROP TABLE IF EXISTS mart_orders;
CREATE TABLE mart_orders AS
SELECT
    order_id,
    user_id,
    DATE(timestamp) AS order_date,
    timestamp AS order_timestamp,
    total_amount,
    payment_method,
    platform,
    (extra_data->>'discount_amount')::NUMERIC(12,2) AS discount_amount,
    (extra_data->>'shipping_fee')::NUMERIC(12,2) AS shipping_fee,
    extra_data->>'coupon_code' AS coupon_code
FROM raw_events
WHERE event_type = 'purchase'
  AND order_id IS NOT NULL;

CREATE INDEX idx_mart_orders_date ON mart_orders(order_date);
CREATE INDEX idx_mart_orders_user ON mart_orders(user_id);
