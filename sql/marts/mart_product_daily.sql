-- ============================================================
-- 상품별 일별 행동 마트 (증분 적재)
-- purchase 이벤트의 상품 정보는 extra_data(JSONB)에서 추출
-- ============================================================

DELETE FROM mart_product_daily WHERE activity_date = '{{ ds }}';

INSERT INTO mart_product_daily (
    product_id, activity_date,
    click_count, cart_add_count, purchase_count,
    revenue, units_sold, unique_viewers, unique_buyers, conversion_rate
)
WITH purchase_items AS (
    -- purchase 이벤트의 extra_data JSONB에서 상품 단위로 언네스트
    SELECT
        user_id,
        timestamp,
        (item->>'product_id')              AS product_id,
        (item->>'quantity')::INTEGER       AS quantity,
        (item->>'unit_price')::NUMERIC     AS unit_price
    FROM raw_events,
         jsonb_array_elements(extra_data->'products') AS item
    WHERE event_type = 'purchase'
      AND DATE(timestamp) = '{{ ds }}'
),
product_events AS (
    -- click/add_to_cart 이벤트 (product_id 직접 참조)
    SELECT product_id, user_id, event_type, quantity, unit_price
    FROM raw_events
    WHERE product_id IS NOT NULL
      AND event_type IN ('click', 'add_to_cart')
      AND DATE(timestamp) = '{{ ds }}'

    UNION ALL

    -- purchase 이벤트 (JSONB에서 추출한 상품 단위)
    SELECT product_id, user_id, 'purchase' AS event_type, quantity, unit_price
    FROM purchase_items
)
SELECT
    product_id,
    '{{ ds }}'::DATE AS activity_date,
    COUNT(CASE WHEN event_type = 'click'       THEN 1 END)  AS click_count,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END)  AS cart_add_count,
    COUNT(CASE WHEN event_type = 'purchase'    THEN 1 END)  AS purchase_count,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN unit_price * quantity END), 0) AS revenue,
    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN quantity END), 0)              AS units_sold,
    COUNT(DISTINCT CASE WHEN event_type = 'click'    THEN user_id END) AS unique_viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_buyers,
    ROUND(
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'click' THEN user_id END), 0) * 100, 2
    ) AS conversion_rate
FROM product_events
GROUP BY product_id;
