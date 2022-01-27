truncate table pres.report_weblog_access;--statement_end
insert into pres.report_weblog_access
select access.ip_address
, access.username
, customer.customer_full_name as customer_name
, customer.date_of_birth as customer_date_of_birth
, customer.country as customer_country
, access.request_timestamp
, access.request_timestamp_converted
, access.country
, access.device
, case when access.request_timestamp_converted = '1900-01-01' then 'Y' else 'N' end as request_timestamp_has_issu
, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from store_weblog.access
left join store_mysql.customers customer on access.username = customer.username;--statement_end