select count(product_price * product_quantity) number_of_sales, sum(o.product_quantity) product_quantity_count,
case 
	when store_type != 'Web Portal' then 'Offline'
	else 'Web'
end as location
from orders_table o
left join dim_products p
on o.product_code = p.product_code
left join dim_store_details s
on o.store_code = s.store_code
group by location