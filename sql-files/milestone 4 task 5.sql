select store_type, round(sum(product_price * product_quantity)::numeric, 2) total_sales, 
round(count(*) * 100/sum(count(*)) OVER ():: numeric, 2) percentage_total
from orders_table o
left join dim_store_details s
on o.store_code = s.store_code
left join dim_products p
on o.product_code = p.product_code
group by store_type
order by total_sales desc