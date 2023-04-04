WITH sales_rate AS 
(SELECT year, ((year|| '-'|| month ||'-'|| day ||' '|| timestamp)::timestamp -
(LEAD(year|| '-'|| month ||'-'|| day ||' '|| timestamp::time)
OVER (ORDER BY year|| '-'|| month ||'-'|| day ||' '|| timestamp::time DESC ))::timestamp)
AS time_taken
FROM dim_date_times)

SELECT year, AVG(time_taken) AS actual_time_taken FROM sales_rate
GROUP BY year
ORDER BY actual_time_taken DESC
