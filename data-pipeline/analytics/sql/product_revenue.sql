SELECT 
    p.product_key,
    p.title AS product_name,
    COUNT(DISTINCT o.order_id) AS orders_count,
    CAST(SUM(oi.quantity) AS BIGINT) AS units_sold,
    SUM(oi.line_total_amount) AS revenue
FROM fact_order_items oi
JOIN fact_orders o ON oi.order_key = o.order_key
JOIN dim_products p ON oi.product_key = p.product_key
WHERE o.is_paid = TRUE
GROUP BY p.product_key, p.title
ORDER BY revenue DESC, p.product_key ASC;
