SELECT
    p.title AS product,
    COUNT(v.view_key) AS view_count,
    COUNT(DISTINCT v.session_token) AS unique_sessions
FROM fact_product_views v
JOIN dim_products p ON v.product_key = p.product_key
GROUP BY p.title
ORDER BY view_count DESC, product ASC;
