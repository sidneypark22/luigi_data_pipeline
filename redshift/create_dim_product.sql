DROP table if exists pres.dim_product;
CREATE TABLE IF NOT EXISTS pres.dim_product
(
	product_key INT IDENTITY(0,1) PRIMARY KEY
	,product_code VARCHAR(50)
	,product_name VARCHAR(100)
	,supplier_id VARCHAR(46)
	,default_price NUMERIC(10,2)
	,created_datetime DATETIME
	,updated_datetime DATETIME
	,is_deleted INTEGER
	,etl_row_valid_from date
	,etl_row_valid_to date
	,etl_row_is_current integer
	,etl_created_datetime DATETIME
	,etl_updated_datetime DATETIME
	,unique(product_code, supplier_id)
)
DISTKEY(product_key)
SORTKEY(product_code)
;

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
select 'Unknown' as product_code 
	, 'Unknown' as product_name 
	, 'Unknown' as supplier_id 
	, -1 as default_price 
	, cast('1900-01-01' as date) as created_datetime 
	, cast('1900-01-01' as date) as updated_datetime 
	, 0 as is_deleted 
	, cast('1900-01-01' as date) as etl_row_valid_from
	, cast('9999-12-31' as date) as etl_row_valid_to
	, 1 as etl_row_is_current
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
where not exists (select 1 from pres.dim_product limit 1)
;

/*
 * select product_code 
	, product_name 
	, supplier_id 
	, default_price 
	, created_datetime 
	, updated_datetime 
	, is_deleted 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
from store_mysql.products p 
;
*/

