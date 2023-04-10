SELECT s.store_type, ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, 
ROUND(COUNT(*) * 100/SUM(COUNT(*)) OVER ():: numeric, 2) AS percentage_total
FROM orders_table AS o
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
GROUP BY store_type
ORDER BY total_sales DESC;