SELECT ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales, s.store_type, s.country_code
FROM orders_table AS o
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
GROUP BY s.store_type, s.country_code
HAVING s.country_code = 'DE'
ORDER BY total_sales ASC;