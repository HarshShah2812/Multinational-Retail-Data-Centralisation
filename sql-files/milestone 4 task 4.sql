SELECT COUNT(*) AS number_of_sales, SUM(o.product_quantity) AS product_quantity_count,
CASE 
	WHEN s.store_type != 'Web Portal' THEN 'Offline'
	ELSE 'Web'
END AS location
FROM orders_table AS o
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
GROUP BY location;