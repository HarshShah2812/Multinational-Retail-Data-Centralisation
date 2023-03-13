INSERT INTO dim_card_details (card_number)
SELECT DISTINCT card_number 
FROM orders_table
WHERE NOT EXISTS (SELECT * FROM dim_card_details WHERE dim_card_details.card_number = orders_table.card_number);
