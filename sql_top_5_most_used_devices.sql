select device, count(*) as request_count
from pres.report_weblog_access
where request_timestamp_converted >= '2021-01-01' and request_timestamp_converted < '2022-01-01'
group by device
order by request_count desc
limit 5;