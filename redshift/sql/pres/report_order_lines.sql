truncate table pres.report_order_lines;--statement_end
insert into pres.report_order_lines
select oh.order_no
, ol.order_line_no
, ol.order_date
, ol.product_code
, product.product_name
, product.default_price
, ol.supplier_id as supplier_company_id
, supplier.company_name as supplier_company_name
, supplier.country as supplier_company_country
, oh.company_id as seller_company_id
, seller.company_name as seller_company_name
, seller.country as seller_company_country
, customer.customer_document_no as customer_id
, customer.customer_full_name as customer_name
, customer.username as customer_username
, customer.date_of_birth as customer_date_of_birth
, customer.country as customer_country
, ol.unit_price
, ol.quantity
, ol.line_total
, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from store_mysql.order_lines ol
join store_mysql.order_headers oh on ol.order_no = oh.order_no
left join store_mysql.companies supplier on ol.supplier_id = supplier.company_id
left join store_mysql.companies seller on oh.company_id = seller.company_id
left join store_mysql.customers customer on oh.customer_document_no = customer.customer_document_no
left join store_mysql.products product on ol.product_code = product.product_code and ol.supplier_id = product.supplier_id
where ol.is_deleted = 0
and oh.is_deleted = 0;--statement_end