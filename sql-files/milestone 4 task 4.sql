select count(product_price * product_quantity) number_of_sales, sum(o.product_quantity) product_quantity_count,
case 
	when store_type != 'Web Portal' then 'Offline'
	else 'Web'
end as location
from dim_products p
join orders_table o
on p.product_code = o.product_code
join dim_store_details s
on o.store_code = s.store_code
group by location