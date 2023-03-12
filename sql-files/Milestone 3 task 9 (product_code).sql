INSERT INTO dim_products (product_code)
SELECT DISTINCT product_code 
FROM orders_table
WHERE NOT EXISTS (SELECT * FROM dim_products WHERE dim_products.product_code = orders_table.product_code);
