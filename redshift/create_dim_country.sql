DROP table if exists pres.dim_country;
CREATE TABLE IF NOT EXISTS pres.dim_country
(
  	country_key INT IDENTITY(0,1) PRIMARY KEY
	,country VARCHAR(50)
	,country_name VARCHAR(100)
	,is_deleted INTEGER
	,etl_created_datetime DATETIME
	,etl_updated_datetime DATETIME
	,unique (country)
)
DISTKEY(country_key)
SORTKEY(country)
;

insert into pres.dim_country (
	country
	, country_name
	, is_deleted
	, etl_created_datetime
	, etl_updated_datetime
)
select 'Unknown' as country
	, 'Unknown' as country_name
	, 0 as is_deleted
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
where not exists (select 1 from pres.dim_country limit 1)
;

insert into pres.dim_country (country, country_name, is_deleted, etl_created_datetime, etl_updated_datetime)
select 'US', 'United States', 0, date_trunc('second', cast(current_timestamp as datetime)), date_trunc('second', cast(current_timestamp as datetime))
union
select 'CA', 'Canada', 0, date_trunc('second', cast(current_timestamp as datetime)), date_trunc('second', cast(current_timestamp as datetime))
union
select 'AU', 'Australia', 0, date_trunc('second', cast(current_timestamp as datetime)), date_trunc('second', cast(current_timestamp as datetime))
union
select 'CN', 'China', 0, date_trunc('second', cast(current_timestamp as datetime)), date_trunc('second', cast(current_timestamp as datetime))
union
select 'NZ', 'New Zealand', 0, date_trunc('second', cast(current_timestamp as datetime)), date_trunc('second', cast(current_timestamp as datetime))
;

