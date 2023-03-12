select locality, count(store_code) total_no_stores
from dim_store_details
group by locality
order by total_no_stores desc limit 7;
