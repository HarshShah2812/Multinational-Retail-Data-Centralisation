select store_type, sum(product_price * product_quantity) total_sales, sum(product_price * product_quantity) * 100/sum(sum(product_price * product_quantity)) OVER () percentage_total
from dim_store_details s
join orders_table o
on s.store_code = o.store_code
join dim_products p
on o.product_code = p.product_code
group by store_type
order by total_sales desc