begin transaction;

with cte_store as (
    select ip_address
        , request_timestamp
        , request_line
        , status_code
        , response_size
        , user_agent
        , 0 as is_deleted
    from store_weblog.access
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_weblog_access_attributes
set request_line = store.request_line
, status_code = store.status_code
, response_size = store.response_size
, user_agent = store.user_agent
, is_deleted = store.is_deleted
, etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_weblog_access_attributes target
on store.ip_address = target.ip_address
and store.request_timestamp = target.request_timestamp;

insert into pres.dim_weblog_access_attributes (
    ip_address
    , request_timestamp
    , request_line
    , status_code
    , response_size
    , user_agent
    , is_deleted
    , etl_created_datetime
    , etl_updated_datetime
)
with cte_store as (
    select ip_address
        , request_timestamp
        , request_line
        , status_code
        , response_size
        , user_agent
        , 0 as is_deleted
    from store_weblog.access
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
select store.*
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from cte_store store
left join pres.dim_weblog_access_attributes target
on store.ip_address = target.ip_address
and store.request_timestamp = target.request_timestamp
where target.etl_updated_datetime is null;

commit transaction;



