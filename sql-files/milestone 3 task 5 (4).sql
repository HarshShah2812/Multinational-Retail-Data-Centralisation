ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::float,
ALTER COLUMN weight TYPE FLOAT USING weight::float,
ALTER COLUMN "ean" TYPE VARCHAR(20) USING "ean"::varchar(20),
ALTER COLUMN product_code TYPE VARCHAR(15) USING product_code::varchar(15),
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING still_available::bool,
ALTER COLUMN weight_class TYPE VARCHAR(20) USING weight_class::varchar(20)
