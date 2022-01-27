create database dw_dev;

create schema if not exists land;
create schema if not exists store_mysql;
create schema if not exists store_weblog;
--create schema if not exists stage;
create schema if not exists pres;--presentation
--create schema if not exists control;

-- External Schema for S3 data
-- In order to create external schema and tables and read data from S3,
-- the role needs to have access to Redshift, Glue and S3.
create external schema land_source_s3
from data catalog
database 'dw_dev'
iam_role 'arn:aws:iam::285958899685:role/RedshiftRole'
create external database if not exists;

-- #########################################
-- Landing Tables
-- #########################################

-- Create mysql companies landing table
drop table if exists land_source_s3.mysql_companies;
create external table land_source_s3.mysql_companies (
  company_id varchar(46),
  company_name varchar(255),
  country varchar(100),
  is_supplier int,
  created_datetime datetime,
  updated_datetime datetime,
  is_deleted int
)
partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/companies/'
;


-- Create mysql customers landing table
drop table if exists land_source_s3.mysql_customers;
create external table land_source_s3.mysql_customers (
    customer_document_no varchar(8),
    customer_full_name varchar(100),
    username varchar(50),
    date_of_birth date,
    address_line_1 varchar(100),
    address_line_2 varchar(100),
    address_line_3 varchar(100),
    city varchar(50),
    state varchar(50),
    country varchar(50),
    post_code varchar(50),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int
    )
    partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/customers/'

-- Create mysql products landing table
drop table if exists land_source_s3.mysql_products;
create external table land_source_s3.mysql_products (
        product_code varchar(50),
        product_name varchar(100),
        supplier_id varchar(46),
        default_price decimal(10,2),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted int
    )
      partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/products/';

-- Create mysql product_prices landing table
drop table if exists land_source_s3.mysql_product_prices;
create external table land_source_s3.mysql_product_prices (
        product_code varchar(50),
        selling_company_id varchar(46),
        price decimal(10,2),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted int
)
      partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/product_prices/';

-- Create mysql order_lines landing table
drop table if exists land_source_s3.mysql_order_lines;
create external table land_source_s3.mysql_order_lines (
    order_no varchar(16),
    order_line_no varchar(4),
    order_date date,
    product_code varchar(50),
    supplier_id varchar(46),
    unit_price decimal(10,2),
    quantity int,
    line_total decimal(20,4),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int
)
partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/order_lines/';

-- Create mysql order_headers landing table
drop table if exists land_source_s3.mysql_order_headers;
create external table land_source_s3.mysql_order_headers (
        order_no varchar(16),
        order_date date,
        company_id varchar(46),
        customer_document_no varchar(8),
        total decimal(20,4),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted int
)
      partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/mysql/order_headers/';


-- Create weblog access landing table
drop table if exists land_source_s3.weblog_access;
create external table land_source_s3.weblog_access (
        ip_address varchar(15),
        request_identity varchar(50),
        username varchar(50),
        request_timestamp varchar(30),
        request_line varchar(65535),
        status_code varchar(3),
        response_size bigint,
        referer varchar(250),
        user_agent varchar(65535),
        country varchar(2),
        device varchar(50),
        request_timestamp_converted varchar(19)
)
partitioned by (execution_sequence int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
location 's3://dw-dev-source/weblog/access/';



-- #########################################
-- Store Tables
-- #########################################

-- Create mysql companies store table
drop table if exists store_mysql.companies;
create table store_mysql.companies (
  company_id varchar(46),
  company_name varchar(255),
  country varchar(100),
  is_supplier int,
  created_datetime datetime,
  updated_datetime datetime,
  is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (company_id)
) distkey (company_id)
;
;


-- Create mysql customers store table
drop table if exists store_mysql.customers;
create table store_mysql.customers (
    customer_document_no varchar(8),
    customer_full_name varchar(100),
    username varchar(50),
    date_of_birth date,
    address_line_1 varchar(100),
    address_line_2 varchar(100),
    address_line_3 varchar(100),
    city varchar(50),
    state varchar(50),
    country varchar(50),
    post_code varchar(50),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (customer_document_no)
) distkey (customer_document_no)
;

-- Create mysql products store table
drop table if exists store_mysql.products;
create table store_mysql.products (
    product_code varchar(50),
    product_name varchar(100),
    supplier_id varchar(46),
    default_price decimal(10,2),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (product_code, supplier_id)
) distkey (product_code)
;

-- Create mysql product_prices store table
drop table if exists store_mysql.product_prices;
create table store_mysql.product_prices (
	product_code varchar(50),
    selling_company_id varchar(46),
    price decimal(10,2),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (product_code, selling_company_id)
) distkey (product_code)
;


-- Create mysql order_lines store table
drop table if exists store_mysql.order_lines;
create table store_mysql.order_lines (
    order_no varchar(16),
    order_line_no varchar(4),
    order_date date,
    product_code varchar(50),
    supplier_id varchar(46),
    unit_price decimal(10,2),
    quantity int,
    line_total decimal(20,4),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (order_no, order_line_no)
) distkey (order_date)
;



-- Create mysql order_headers store table
drop table if exists store_mysql.order_headers;
create table store_mysql.order_headers (
    order_no varchar(16),
    order_date date,
    company_id varchar(46),
    customer_document_no varchar(8),
    total decimal(20,4),
    created_datetime datetime,
    updated_datetime datetime,
    is_deleted int,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (order_no)
) distkey (order_date)
;



-- Create weblog access store table
drop table if exists store_weblog.access;
create table store_weblog.access (
    ip_address varchar(15),
    request_identity varchar(50),
    username varchar(50),
    request_timestamp varchar(30),
    request_line varchar(65535),
    status_code varchar(3),
	response_size bigint,
	referer varchar(250),
	user_agent varchar(65535),
	country varchar(2),
	device varchar(50),
      request_timestamp_converted varchar(19),
	etl_created_datetime datetime,
	etl_updated_datetime datetime,
	unique (ip_address, request_timestamp)
)
distkey (request_timestamp_converted)
;




-- #########################################
-- Presentation Tables
-- #########################################

create table if not exists pres.report_order_lines (
	order_no varchar(16)
	,order_line_no varchar(4)
	,order_date date
	,product_code varchar(50)
	,product_name varchar(100)
	,default_price decimal(10,2)
	,supplier_company_id varchar(46)
	,supplier_company_name varchar(255)
	,supplier_company_country varchar(100)
	,seller_company_id varchar(46)
	,seller_company_name varchar(255)
	,seller_company_country varchar(100)
	,customer_id varchar(8)
	,customer_name varchar(100)
	,customer_username varchar(50)
	,customer_date_of_birth date
	,customer_country varchar(50)
	,unit_price decimal(10,2)
	,quantity integer
	,line_total decimal(20,4)
	,etl_created_datetime datetime
	,etl_updated_datetime datetime
    ,unique (order_no, order_line_no)
)
distkey(order_no)
sortkey(order_date)
;


create table if not exists pres.report_weblog_access
(
	ip_address varchar(15)
	,username varchar(50)
	,customer_name varchar(100)
	,customer_date_of_birth date
	,customer_country varchar(50)
	,request_timestamp varchar(30)
	,request_timestamp_converted varchar(19)
	,country varchar(2)
	,device varchar(50)
	,request_timestamp_has_issue varchar(1)
	,etl_created_datetime datetime
	,etl_updated_datetime datetime
    ,unique (ip_address, request_timestamp)
)
distkey(request_timestamp)
sortkey(request_timestamp)
;
