-- Simply full load as I am running out of time before my son gets upset
truncate table pres.fact_order_line;
 
insert into pres.fact_order_line
select order_lines.order_no
, order_lines.order_line_no
, coalesce(dim_date.date_key, 0) as order_date_key
, coalesce(dim_company.company_key, 0) as company_key
, coalesce(dim_customer.customer_key, 0) as customer_key
, coalesce(dim_country.country_key, 0) as customer_country_key
, coalesce(dim_supplier.supplier_key, 0) as supplier_key
, coalesce(dim_product.product_key, 0) as product_key
, order_lines.unit_price
, order_lines.quantity
, order_lines.line_total
, order_lines.created_datetime
, order_lines.updated_datetime
, order_lines.is_deleted
, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from store_mysql.order_lines
join store_mysql.order_headers on order_lines.order_no = order_headers.order_no 
left join pres.dim_date on order_headers.order_date = dim_date.date
left join pres.dim_company on order_headers.company_id = dim_company.company_id
left join pres.dim_customer on order_headers.customer_document_no = dim_customer.customer_document_no
left join pres.dim_country on dim_customer.country = dim_country.country
left join pres.dim_supplier on order_lines.supplier_id = dim_supplier.supplier_id
left join pres.dim_product -- scd2 dimension
on order_lines.product_code = dim_product.product_code 
and order_lines.supplier_id = dim_product.supplier_id
and order_lines.order_date between dim_product.etl_row_valid_from and dim_product.etl_row_valid_to
--where etl_updated_datetime between {etl_start_watermark} and {etl_end_watermark}
;




