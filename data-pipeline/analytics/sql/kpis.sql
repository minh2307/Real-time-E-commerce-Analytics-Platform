WITH order_kpis AS (
    SELECT 
        COUNT(order_key) AS total_orders,
        CAST(SUM(CASE WHEN is_paid THEN 1 ELSE 0 END) AS BIGINT) AS paid_orders,
        SUM(gmv_amount) AS gmv
    FROM fact_orders
),
item_kpis AS (
    SELECT 
        CAST(SUM(quantity) AS BIGINT) AS total_items_sold
    FROM fact_order_items
),
view_kpis AS (
    SELECT 
        COUNT(view_key) AS total_product_views
    FROM fact_product_views
)
SELECT 
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.paid_orders, 0) AS paid_orders,
    COALESCE(o.gmv, 0.0) AS gmv,
    COALESCE(i.total_items_sold, 0) AS total_items_sold,
    COALESCE(v.total_product_views, 0) AS total_product_views
FROM order_kpis o
CROSS JOIN item_kpis i
CROSS JOIN view_kpis v;
