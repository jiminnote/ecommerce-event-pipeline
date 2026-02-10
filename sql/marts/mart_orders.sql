-- ============================================================
-- 주문 마트 (증분 적재)
-- 주문 단위 상세 + extra_data에서 할인/배송비/쿠폰 추출
-- ============================================================

DELETE FROM mart_orders WHERE order_date = '{{ ds }}';

INSERT INTO mart_orders (
    order_id, user_id, order_date, order_timestamp,
    total_amount, payment_method, platform,
    discount_amount, shipping_fee, coupon_code, item_count
)
SELECT
    order_id,
    user_id,
    DATE(timestamp)    AS order_date,
    timestamp          AS order_timestamp,
    total_amount,
    payment_method,
    platform,
    COALESCE((extra_data->>'discount_amount')::NUMERIC(12,2), 0) AS discount_amount,
    COALESCE((extra_data->>'shipping_fee')::NUMERIC(12,2), 0)    AS shipping_fee,
    extra_data->>'coupon_code'                                    AS coupon_code,
    jsonb_array_length(COALESCE(extra_data->'products', '[]'))    AS item_count
FROM raw_events
WHERE event_type = 'purchase'
  AND order_id IS NOT NULL
  AND DATE(timestamp) = '{{ ds }}';
