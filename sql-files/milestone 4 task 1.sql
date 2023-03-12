select country_code as country, count(country_code) as total_no_stores
from dim_store_details
group by country
order by total_no_stores desc