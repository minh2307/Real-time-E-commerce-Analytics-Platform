SELECT
    COALESCE(c.email, 'Unknown') AS customer,
    COUNT(o.order_key) AS orders_count,
    CAST(SUM(CASE WHEN o.is_paid THEN 1 ELSE 0 END) AS BIGINT) AS paid_orders_count,
    SUM(o.gmv_amount) AS gmv
FROM fact_orders o
LEFT JOIN dim_customers c ON o.customer_key = c.customer_key
GROUP BY c.email
ORDER BY gmv DESC, customer ASC;
