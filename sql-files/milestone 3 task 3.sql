ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT using longitude::float,
ALTER COLUMN locality TYPE VARCHAR(255) using locality::varchar(255),
ALTER COLUMN store_code TYPE VARCHAR(15) using store_code::varchar(15),
ALTER COLUMN staff_numbers TYPE SMALLINT using staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE using opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255) using store_type::varchar(255),
ALTER COLUMN latitude TYPE FLOAT using latitude::float,
ALTER COLUMN country_code TYPE VARCHAR(2) using country_code::varchar(2),
ALTER COLUMN continent TYPE VARCHAR(255) using continent::varchar(255)
