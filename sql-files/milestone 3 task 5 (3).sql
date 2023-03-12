UPDATE dim_products
SET still_available = False
WHERE still_available = 'Removed'
