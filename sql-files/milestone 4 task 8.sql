select round(sum(product_price * product_quantity)::numeric, 2) total_sales, store_type, country_code
from orders_table o
left join dim_products p
on o.product_code = p.product_code
left join dim_store_details s
on o.store_code = s.store_code
group by store_type, country_code
having country_code = 'DE'
order by total_sales asc;