SELECT sum(product_price * product_quantity) total_sales, year, month
from dim_products p
join orders_table o
on p.product_code = o.product_code
join dim_date_times dt
on o.date_uuid = dt.date_uuid
group by year, month
order by total_sales desc limit 5;