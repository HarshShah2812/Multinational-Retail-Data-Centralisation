INSERT INTO dim_store_details (store_code)
SELECT DISTINCT store_code 
FROM orders_table
WHERE NOT EXISTS (SELECT * FROM dim_store_details WHERE dim_store_details.store_code = orders_table.store_code);
