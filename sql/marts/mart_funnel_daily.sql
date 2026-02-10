-- ============================================================
-- 일별 전환 퍼널 마트 (증분 적재)
-- 플랫폼별 단계별 전환율 집계
-- ============================================================

DELETE FROM mart_funnel_daily WHERE funnel_date = '{{ ds }}';

INSERT INTO mart_funnel_daily (
    funnel_date, platform,
    step1_viewers, step2_clickers, step3_cart_adders, step4_purchasers,
    view_to_click_rate, click_to_cart_rate,
    cart_to_purchase_rate, overall_conversion_rate
)
SELECT
    DATE(timestamp) AS funnel_date,
    platform,
    COUNT(DISTINCT CASE WHEN event_type = 'page_view'   THEN user_id END) AS step1_viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'click'       THEN user_id END) AS step2_clickers,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS step3_cart_adders,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase'    THEN user_id END) AS step4_purchasers,
    -- 단계별 전환율
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
    -- 전체 전환율 (조회 → 구매)
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END), 0) * 100, 2
    ) AS overall_conversion_rate
FROM raw_events
WHERE DATE(timestamp) = '{{ ds }}'
GROUP BY DATE(timestamp), platform;
