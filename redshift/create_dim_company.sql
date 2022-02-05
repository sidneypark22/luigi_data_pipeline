DROP TABLE if exists pres.dim_company;
CREATE TABLE IF NOT EXISTS pres.dim_company
(
	company_key INT IDENTITY(0,1) PRIMARY KEY
	,company_id VARCHAR(46)
	,company_name VARCHAR(255)
	,country VARCHAR(100)
	,is_supplier INTEGER
	,created_datetime DATETIME
	,updated_datetime DATETIME
	,is_deleted INTEGER
	,etl_created_datetime DATETIME
	,etl_updated_datetime DATETIME
)
DISTKEY (company_key)
sortkey (company_id)
;


insert into pres.dim_company (
	company_id
	,company_name
	,country
	,is_supplier
	,created_datetime
	,updated_datetime
	,is_deleted
	,etl_created_datetime
	,etl_updated_datetime
)
select 'Unknown' as company_id
	, 'Unknown' as company_name
	, 'Unknown' as country
	, -1 as is_supplier
	, cast('1900-01-01' as date) as created_datetime
	, cast('1900-01-01' as date) as updated_datetime
	, 0 as is_deleted
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
where not exists (select 1 from pres.dim_company)
;

/*
select company_id 
	, company_name
	, country 
	, is_supplier 
	, created_datetime 
	, updated_datetime 
	, is_deleted 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
from store_mysql.companies c 
;
*/

