DROP TABLE if exists pres.fact_order_line;
CREATE TABLE IF NOT EXISTS pres.fact_order_line
(
	order_no VARCHAR(16)
	,order_line_no VARCHAR(4)
	,order_date_key INTEGER
	,company_key INTEGER
	,customer_key INTEGER
	,customer_country_key INTEGER
	,supplier_key INTEGER
	,product_key INTEGER
	,unit_price NUMERIC(10,2)
	,quantity INTEGER
	,line_total NUMERIC(20,4)
	,created_datetime datetime
	,updated_datetime datetime
	,is_deleted INTEGER
	,etl_created_datetime datetime
	,etl_updated_datetime datetime
	,unique(order_no, order_line_no)
)
distkey(product_key)
sortkey(order_date_key)
;
