begin transaction;

with cte_store as (
    select company_id 
        , company_name
        , country 
        , is_supplier 
        , created_datetime 
        , updated_datetime 
        , is_deleted
    from store_mysql.companies
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_company
set company_name = store.company_name
, country = store.country
, is_supplier = store.is_supplier
, created_datetime = store.created_datetime
, updated_datetime = store.updated_datetime
, is_deleted = store.is_deleted
, etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_company target
on store.company_id = target.company_id;

insert into pres.dim_company (
  company_id 
  , company_name
  , country 
  , is_supplier 
  , created_datetime 
  , updated_datetime 
  , is_deleted
  , etl_created_datetime
  , etl_updated_datetime
)
with cte_store as (
    select company_id 
        , company_name
        , country 
        , is_supplier 
        , created_datetime 
        , updated_datetime 
        , is_deleted
    from store_mysql.companies
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
select store.*
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from cte_store store
left join pres.dim_company target
on store.company_id = target.company_id
where target.etl_updated_datetime is null;

commit transaction;
