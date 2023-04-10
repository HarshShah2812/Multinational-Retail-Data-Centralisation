SELECT SUM(staff_numbers) AS total_staff_numbers,
CASE 
	WHEN country_code = 'DE' THEN 'DE'
	WHEN country_code = 'US' THEN 'US'
	ELSE 'GB'
END AS country_code
FROM dim_store_details 
GROUP BY country_code
ORDER BY total_staff_numbers DESC;