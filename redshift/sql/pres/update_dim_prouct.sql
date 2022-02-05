begin transaction;

-- Retire existing record
with cte_store as (
    select product_code 
	, product_name 
	, supplier_id 
	, default_price 
	, created_datetime 
	, updated_datetime 
	, is_deleted
    from store_mysql.products
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_product
set etl_row_is_current = 0
, etl_row_valid_to = cast((current_date - 1) as date)
, etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_product target
on store.product_code = target.product_code
and store.supplier_id = target.supplier_id
and target.etl_row_is_current = 1
--SCD2 tracking fields
where store.product_name <> target.product_name
or store.default_price <> target.default_price;


-- Update existing record
with cte_store as (
    select product_code 
	, product_name 
	, supplier_id 
	, default_price 
	, created_datetime 
	, updated_datetime 
	, is_deleted
    from store_mysql.products
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_product
set product_name = store.product_name
, default_price = store.default_price
, created_datetime = store.created_datetime
, updated_datetime = store.updated_datetime
, is_deleted = store.is_deleted
, etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_product target
on store.product_code = target.product_code
and store.supplier_id = target.supplier_id
and target.etl_row_is_current = 1
--SCD2 tracking fields
where not (
    store.product_name <> target.product_name
or store.default_price <> target.default_price
);

-- Insert new records / SCD2 change tracked records
insert into pres.dim_product (
    product_code 
	, product_name 
	, supplier_id 
	, default_price 
	, created_datetime 
	, updated_datetime 
	, is_deleted 
	, etl_row_valid_from
	, etl_row_valid_to
	, etl_row_is_current
	, etl_created_datetime 
	, etl_updated_datetime 
)
with cte_store as (
    select product_code 
        , product_name 
        , supplier_id 
        , default_price 
        , created_datetime 
        , updated_datetime 
        , is_deleted
    from store_mysql.products
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
, cte_store_new as (
    select store.*
        , cast('1900-01-01' as date) as etl_row_valid_from
        , cast('9999-12-31' as date) as etl_row_valid_to
        , 1 as etl_row_is_current
        , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
        , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
    from cte_store store
    left join pres.dim_product target
    on store.product_code = target.product_code
    and store.supplier_id = target.supplier_id
    --and target.etl_row_is_current = 1
    where target.etl_updated_datetime is null
)
, cte_store_change_tracked as (
    select store.*
        , cast(current_date as date) as etl_row_valid_from
        , cast('9999-12-31' as date) as etl_row_valid_to
        , 1 as etl_row_is_current
        , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
        , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
    from cte_store store
    left join pres.dim_product target
    on store.product_code = target.product_code
    and store.supplier_id = target.supplier_id
    and target.etl_row_is_current = 1
    where target.etl_updated_datetime is null
    and not exists (
        select 1
        from cte_store_new
        where store.product_code = cte_store_new.product_code
        and store.supplier_id = cte_store_new.supplier_id
    )
)
select *
from cte_store_new
union all
select *
from cte_store_change_tracked
;

commit transaction;

