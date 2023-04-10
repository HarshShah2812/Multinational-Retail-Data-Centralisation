SELECT ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, dt.year, dt.month
FROM orders_table AS o
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
LEFT JOIN dim_date_times AS dt
ON o.date_uuid = dt.date_uuid
GROUP BY dt.year, dt.month
ORDER BY total_sales DESC LIMIT 10;