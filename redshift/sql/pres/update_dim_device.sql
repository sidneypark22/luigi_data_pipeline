begin transaction;

with cte_store as (
    select distinct device
        , 0 as is_deleted
    from store_weblog.access
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_device
set is_deleted = store.is_deleted
,etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_device target
on store.device = target.device;

insert into pres.dim_device (
  device
  ,is_deleted
  ,etl_created_datetime
  ,etl_updated_datetime
)
with cte_store as (
    select distinct device
        , 0 as is_deleted
    from store_weblog.access
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
select store.*
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from cte_store store
left join pres.dim_device target
on store.device = target.device
where target.etl_updated_datetime is null;

commit transaction;

