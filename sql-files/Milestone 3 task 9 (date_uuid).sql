INSERT INTO dim_date_times (date_uuid)
SELECT DISTINCT date_uuid 
FROM orders_table
WHERE NOT EXISTS (SELECT * FROM dim_date_times WHERE dim_date_times.date_uuid = orders_table.date_uuid);
