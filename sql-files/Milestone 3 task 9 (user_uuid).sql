INSERT INTO dim_users (user_uuid)
SELECT DISTINCT user_uuid 
FROM orders_table
WHERE NOT EXISTS (SELECT * FROM dim_users WHERE dim_users.user_uuid = orders_table.user_uuid);
