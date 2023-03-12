ALTER TABLE dim_date_times
ALTER COLUMN month TYPE CHAR(2) USING month::char(2),
ALTER COLUMN year TYPE CHAR(4) USING year::char(4),
ALTER COLUMN day TYPE CHAR(2) USING day::char(2),
ALTER COLUMN time_period TYPE VARCHAR(15) USING time_period::varchar(15),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid
