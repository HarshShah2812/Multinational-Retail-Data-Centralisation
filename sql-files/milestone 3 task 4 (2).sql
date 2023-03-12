UPDATE dim_products
SET weight_class = CASE WHEN weight < 3 THEN 'Light' WHEN weight BETWEEN 3 AND 40 THEN 'Mid_Sized' WHEN weight BETWEEN 41 and 140 THEN 'Heavy' ELSE 'Truck_Required' 
END