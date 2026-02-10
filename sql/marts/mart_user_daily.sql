-- ============================================================
-- 사용자 일별 행동 요약 마트 (증분 적재)
-- Airflow 템플릿: {{ ds }} = execution_date (YYYY-MM-DD)
-- 멱등성 보장: DELETE → INSERT 패턴
-- ============================================================

DELETE FROM mart_user_daily WHERE activity_date = '{{ ds }}';

INSERT INTO mart_user_daily (
    user_id, activity_date, session_count,
    page_view_count, click_count, add_to_cart_count,
    purchase_count, search_count, total_purchase_amount,
    first_event_at, last_event_at, session_duration_total
)
SELECT
    user_id,
    DATE(timestamp) AS activity_date,
    COUNT(DISTINCT session_id)                                          AS session_count,
    COUNT(CASE WHEN event_type = 'page_view'    THEN 1 END)            AS page_view_count,
    COUNT(CASE WHEN event_type = 'click'        THEN 1 END)            AS click_count,
    COUNT(CASE WHEN event_type = 'add_to_cart'  THEN 1 END)            AS add_to_cart_count,
    COUNT(CASE WHEN event_type = 'purchase'     THEN 1 END)            AS purchase_count,
    COUNT(CASE WHEN event_type = 'search'       THEN 1 END)            AS search_count,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN total_amount END), 0) AS total_purchase_amount,
    MIN(timestamp)                                                      AS first_event_at,
    MAX(timestamp)                                                      AS last_event_at,
    MAX(timestamp) - MIN(timestamp)                                     AS session_duration_total
FROM raw_events
WHERE DATE(timestamp) = '{{ ds }}'
GROUP BY user_id, DATE(timestamp);
