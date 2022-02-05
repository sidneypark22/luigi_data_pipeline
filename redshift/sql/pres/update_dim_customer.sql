begin transaction;

with cte_store as (
    select customer_document_no
          ,customer_full_name
          ,username
          ,date_of_birth
          ,address_line_1
          ,address_line_2
          ,address_line_3
          ,city
          ,state
          ,country
          ,post_code
          ,created_datetime
          ,updated_datetime
          ,is_deleted
    from store_mysql.customers
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
update pres.dim_customer
set customer_full_name = store.customer_full_name
,username = store.username
,date_of_birth = store.date_of_birth
,address_line_1 = store.address_line_1
,address_line_2 = store.address_line_2
,address_line_3 = store.address_line_3
,city = store.city
,state = store.state
,country = store.country
,post_code = store.post_code
,created_datetime = store.created_datetime
,updated_datetime = store.updated_datetime
,is_deleted = store.is_deleted
,etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
from cte_store store
join pres.dim_customer target
on store.customer_document_no = target.customer_document_no;

insert into pres.dim_customer (
  customer_document_no
  ,customer_full_name
  ,username
  ,date_of_birth
  ,address_line_1
  ,address_line_2
  ,address_line_3
  ,city
  ,state
  ,country
  ,post_code
  ,created_datetime
  ,updated_datetime
  ,is_deleted
  ,etl_created_datetime
  ,etl_updated_datetime
)
with cte_store as (
    select customer_document_no
          ,customer_full_name
          ,username
          ,date_of_birth
          ,address_line_1
          ,address_line_2
          ,address_line_3
          ,city
          ,state
          ,country
          ,post_code
          ,created_datetime
          ,updated_datetime
          ,is_deleted
    from store_mysql.customers
    --where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
)
select store.*
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from cte_store store
left join pres.dim_customer target
on store.customer_document_no = target.customer_document_no
where target.etl_updated_datetime is null;

commit transaction;
