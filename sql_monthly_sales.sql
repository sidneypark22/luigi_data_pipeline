select to_char(order_date, 'yyyy-mm') as year_month, sum(quantity) as total_quantity, sum(line_total) as total_amount
from pres.report_order_lines
where order_date >= '2021-01-01' and order_date < '2022-01-01'
group by to_char(order_date, 'yyyy-mm')
order by year_month;