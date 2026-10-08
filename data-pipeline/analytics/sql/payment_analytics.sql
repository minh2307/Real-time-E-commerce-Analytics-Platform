SELECT 
    payment_method,
    status AS payment_status,
    COUNT(payment_id) AS payment_attempts,
    SUM(payment_amount) AS payment_amount
FROM fact_payments
GROUP BY payment_method, status
ORDER BY payment_method, payment_status;
