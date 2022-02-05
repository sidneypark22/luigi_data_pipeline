DROP TABLE if exists pres.dim_weblog_access_attributes;
CREATE TABLE IF NOT EXISTS pres.dim_weblog_access_attributes
(
	weblog_access_key int identity(0,1) primary key
  ,ip_address VARCHAR(15)
	,request_timestamp VARCHAR(30)
	,request_line VARCHAR(65535) 
	,status_code VARCHAR(3)
	,response_size BIGINT
	,user_agent VARCHAR(65535) 	
  ,is_deleted int
  ,etl_created_datetime datetime
  ,etl_updated_datetime datetime
  ,unique(ip_address, request_timestamp)
)
distkey(weblog_access_key)
sortkey(request_timestamp)
;

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
select 'Unknown' as ip_address
	, 'Unknown' as request_timestamp
    , 'Unknown' as request_line
    , 'Unk' as status_code
    , -1 as response_size
    , 'Unknown' as user_agent
    , 0 as is_deleted
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
  	, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
where not exists (select 1 from pres.dim_weblog_access_attributes limit 1)
;

select ip_address
	  , request_timestamp
    , request_line
    , status_code
    , response_size
    , user_agent
from store_weblog.access
;