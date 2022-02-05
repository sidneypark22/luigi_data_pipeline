DROP TABLE if exists pres.dim_customer;
CREATE TABLE IF NOT EXISTS pres.dim_customer
(
	customer_key INT IDENTITY(0,1) PRIMARY KEY
	,customer_document_no VARCHAR(8)
	,customer_full_name VARCHAR(100)
	,username VARCHAR(50)
	,date_of_birth DATE
	,address_line_1 VARCHAR(100)
	,address_line_2 VARCHAR(100)
	,address_line_3 VARCHAR(100)
	,city VARCHAR(50)
	,state VARCHAR(50)
	,country VARCHAR(50)
	,post_code VARCHAR(50)
	,created_datetime DATETIME
	,updated_datetime DATETIME
	,is_deleted INTEGER
	,etl_created_datetime DATETIME
	,etl_updated_datetime DATETIME
    ,unique (customer_document_no)
)
distkey(customer_key)
sortkey(customer_document_no)
;


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
select 'Unknown' as customer_document_no
, 'Unknown' as customer_full_name
, 'Unknown' as username
, cast('1900-01-01' as date) as date_of_birth
, 'Unknown' as address_line_1
, 'Unknown' as address_line_2
, 'Unknown' as address_line_3
, 'Unknown' as city
, 'Unknown' as state
, 'Unknown' as country
, 'Unknown' as post_code
, cast('1900-01-01' as date) as created_datetime
, cast('1900-01-01' as date) as updated_datetime
, 0 as is_deleted
, date_trunc('second', cast(current_timestamp as datetime))etl_created_datetime
, date_trunc('second', cast(current_timestamp as datetime))etl_updated_datetime
where not exists (select 1 from pres.dim_customer)
;

/*
select customer_document_no
	, customer_full_name
	, username
	, date_of_birth
	, address_line_1
	, address_line_2
	, address_line_3
	, city
	, state
	, country
	, post_code
	, created_datetime
	, updated_datetime
	, is_deleted
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
from store_mysql.customers c 
where 1=0
*/