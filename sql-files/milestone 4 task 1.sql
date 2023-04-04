select country_code as country, count(*) as total_no_stores
from dim_store_details
-- where store_type != 'Web Portal'
group by country
order by total_no_stores desc
