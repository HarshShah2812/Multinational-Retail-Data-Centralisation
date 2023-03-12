ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(25) USING card_number::varchar(25),
ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::varchar(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date

