with cte_most_logged_in_country as (
	select country, count(*) as request_count
	from pres.report_weblog_access
	where request_timestamp_converted >= '2021-01-01' and request_timestamp_converted < '2022-01-01'
	group by country
	order by request_count desc
	limit 1
)
, cte_top_10_most_sold_by_quantity as (
	select 'Top 10 Most Sold By Quantity' as aggregation_type, customer_country, product_code, product_name, sum(quantity) as sold_total_quantity, sum(line_total) as sold_total_amount
	from pres.report_order_lines
	where customer_country = (select country from cte_most_logged_in_country)
	group by customer_country, product_code, product_name
	order by sold_total_quantity desc
	limit 10
)
, cte_top_10_most_sold_by_amount as (
	select 'Top 10 Most Sold By Amount' as aggregation_type, customer_country, product_code, product_name, sum(quantity) as sold_quantity, sum(line_total) as sold_total_amount
	from pres.report_order_lines
	where customer_country = (select country from cte_most_logged_in_country)
	group by customer_country, product_code, product_name
	order by sold_total_amount desc
	limit 10
)
select *, row_number() over (order by sold_total_quantity desc) as rank
from cte_top_10_most_sold_by_quantity
union all
select *, row_number() over (order by sold_total_amount desc) as rank
from cte_top_10_most_sold_by_amount
;
 
