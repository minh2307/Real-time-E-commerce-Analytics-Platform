SELECT 
    order_date AS date,
    COUNT(*) AS orders_count,
    CAST(SUM(CASE WHEN is_paid THEN 1 ELSE 0 END) AS BIGINT) AS paid_orders_count,
    SUM(gmv_amount) AS gmv
FROM fact_orders
GROUP BY order_date
ORDER BY date ASC;
