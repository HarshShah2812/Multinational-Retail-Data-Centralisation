select count(product_price * product_quantity) total_sales, store_type, country_code
from dim_products p
join orders_table o
on p.product_code = o.product_code
join dim_store_details s
on o.store_code = s.store_code
group by store_type, country_code
having country_code = 'DE'