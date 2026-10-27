-- =============================================================================
-- LEGACY MONOLITHIC SQL REPORT - DAILY REVENUE RECONCILIATION BENCHMARK
-- Used by Stage 7 WAP Quality Gate (DuckDB / PyArrow) to enforce Delta = 0
-- =============================================================================

INSERT INTO legacy_daily_revenue_report (
    report_date,
    shipping_city,
    order_count,
    distinct_customers,
    gross_merchandise_value,
    total_discount,
    net_revenue,
    computed_at
)
SELECT
    DATE(o.order_ts AT TIME ZONE 'UTC') AS report_date,
    o.shipping_city,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS distinct_customers,
    COALESCE(SUM(o.subtotal_amount), 0.00) AS gross_merchandise_value,
    COALESCE(SUM(o.discount_amount), 0.00) AS total_discount,
    COALESCE(SUM(o.total_amount), 0.00) AS net_revenue,
    NOW() AS computed_at
FROM orders o
WHERE o.status NOT IN ('cancelled', 'failed')
GROUP BY DATE(o.order_ts AT TIME ZONE 'UTC'), o.shipping_city
ON CONFLICT (report_date, shipping_city) 
DO UPDATE SET
    order_count = EXCLUDED.order_count,
    distinct_customers = EXCLUDED.distinct_customers,
    gross_merchandise_value = EXCLUDED.gross_merchandise_value,
    total_discount = EXCLUDED.total_discount,
    net_revenue = EXCLUDED.net_revenue,
    computed_at = EXCLUDED.computed_at;
