drop view if exists pres.dim_supplier;

create view pres.dim_supplier as
select company_key as supplier_key
  , company_id as supplier_id
  , company_name as supplier_name
  , country 
  , is_supplier 
  , created_datetime 
  , updated_datetime 
  , is_deleted
  , etl_created_datetime
  , etl_updated_datetime
from pres.dim_company
where is_supplier = 1
;