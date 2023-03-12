select sum(p.product_price * o.product_quantity) total_sales, dt.month
from orders_table o
left join dim_date_times dt
on o.date_uuid = dt.date_uuid
left join dim_products p
on o.product_code = p.product_code
group by dt.month
order by total_sales desc limit 6;