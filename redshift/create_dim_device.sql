drop table if exists pres.dim_device;
create table if not exists pres.dim_device (
	device_key INT IDENTITY(0,1) PRIMARY KEY
 	,device varchar(50)
 	,is_deleted int
	,etl_created_datetime DATETIME
	,etl_updated_datetime DATETIME
  	,unique(device)
)
distkey(device_key)
sortkey(device)
;

insert into pres.dim_device (device, is_deleted, etl_created_datetime, etl_updated_datetime)
select 'Unknown' as device
	, 0 as is_deleted
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime 
	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime 
where not exists (select 1 from pres.dim_device limit 1)
;

/*
select distinct device
from store_weblog.access
where 1=0
;
*/