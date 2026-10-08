SELECT 
    c.category_key,
    c.name AS category_name,
    COUNT(DISTINCT o.order_id) AS orders_count,
    CAST(SUM(oi.quantity) AS BIGINT) AS units_sold,
    SUM(oi.line_total_amount) AS revenue
FROM fact_order_items oi
JOIN fact_orders o ON oi.order_key = o.order_key
JOIN dim_products p ON oi.product_key = p.product_key
LEFT JOIN dim_categories c ON p.category_key = c.category_key
WHERE o.is_paid = TRUE
GROUP BY c.category_key, c.name
ORDER BY revenue DESC, c.category_key ASC;
