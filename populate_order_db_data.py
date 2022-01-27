import boto3
import mysql.connector

host="order-mgmt-db.czj687xt6h8w.us-east-1.rds.amazonaws.com" # change the host name accordingly
user="admin"
password="password"
database="order_mgmt_db"

orderdb = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database
)

cursor = orderdb.cursor()

### Tables Required ###
"""
Companies
Customers
Suppliers (Companies)
Products
Product Prices
Order Headers
Order Lines
"""

### Create Companies table
cursor.execute('drop table if exists companies;')
cursor.execute(
    """
    create table companies (
        company_id varchar(46),
        company_name varchar(255),
        country varchar(100),
        is_supplier boolean,
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted boolean
    );
    """
)

### Create Customers table
cursor.execute(
    """
    create table customers (
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
        is_deleted boolean
    );
    """
)

### Create Products table
cursor.execute(
    """
    create table products (
        product_code varchar(50),
        product_name varchar(100),
        supplier_id varchar(46),
        default_price decimal(10,2),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted boolean
    );
    """
)

### Create Product Prices table
cursor.execute(
    """
    create table product_prices (
        product_code varchar(50),
        selling_company_id varchar(46),
        price decimal(10,2),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted boolean
    );
    """
)

insert_companies_statment = \
    """
    insert into companies 
    (company_id, company_name, country, is_supplier, created_datetime, updated_datetime, is_deleted)
    values
    (uuid(), 'Company A', 'US', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company B', 'CA', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company C', 'CN', true, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company D', 'AU', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company E', 'NZ', true, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company F', 'US', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company G', 'US', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company H', 'US', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company I', 'AU', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company J', 'AU', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company K', 'CA', false, current_timestamp(), current_timestamp(), false),
    (uuid(), 'Company L', 'CA', false, current_timestamp(), current_timestamp(), false)
    """
cursor.execute(insert_companies_statment)

insert_customers_statment = \
    """
    insert into customers 
    (customer_document_no, customer_full_name, username, date_of_birth, address_line_1, address_line_2, address_line_3, city, state, country, post_code, created_datetime, updated_datetime, is_deleted)
    values
    ('00000001', 'John Jackson', 'user_1', '1972-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000002', 'Maple Wood', 'user_2', '1975-01-01', '', '', '', '', '', 'CA', '1010', current_timestamp(), current_timestamp(), false),
    ('00000003', 'Hao Chen', 'user_3', '1978-01-01', '', '', '', '', '', 'AU', '1010', current_timestamp(), current_timestamp(), false),
    ('00000004', 'Koala King', 'user_4', '1982-01-01', '', '', '', '', '', 'AU', '1010', current_timestamp(), current_timestamp(), false),
    ('00000005', 'Sidney Park', 'user_5', '1985-01-01', '', '', '', '', '', 'CA', '1010', current_timestamp(), current_timestamp(), false),
    ('00000006', 'Jack Smith', 'user_6', '1985-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000007', 'Paul Tait', 'user_7', '1985-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000008', 'Jennifer Olsen', 'user_8', '1985-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000009', 'Michael Jordan', 'user_9', '1985-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000010', 'Tom Cruise', 'user_10', '1985-01-01', '', '', '', '', '', 'US', '1010', current_timestamp(), current_timestamp(), false),
    ('00000011', 'Oliver Sykes', 'user_11', '1985-01-01', '', '', '', '', '', 'CA', '1010', current_timestamp(), current_timestamp(), false),
    ('00000012', 'Justin Bieber', 'user_12', '1985-01-01', '', '', '', '', '', 'CA', '1010', current_timestamp(), current_timestamp(), false),
    ('00000013', 'Alicia Keys', 'user_13', '1985-01-01', '', '', '', '', '', 'AU', '1010', current_timestamp(), current_timestamp(), false),
    ('00000014', 'Taylor Swift', 'user_14', '1985-01-01', '', '', '', '', '', 'AU', '1010', current_timestamp(), current_timestamp(), false)
    """
cursor.execute(insert_customers_statment)

insert_products_statment = \
    """
    insert into products 
    (product_code, product_name, supplier_id, default_price, created_datetime, updated_datetime, is_deleted)
    values
    ('LAPTOP1', 'Nice Laptop 1', '0a3ffe7c-7395-11', '1300', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', 'Nice Laptop 1', '0a3ffe7c-7395-11', '1800', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', 'Nice Laptop 3', '0a3ffe7c-7395-11', '800', current_timestamp(), current_timestamp(), false),
    ('ACC0001', 'Laptop Accessory 1', '0a3ffe7c-7395-11', '30', current_timestamp(), current_timestamp(), false),
    ('ACC0002', 'Laptop Accessory 1', '0a3ffe7c-7395-11', '3', current_timestamp(), current_timestamp(), false),
    ('ACC0003', 'Laptop Accessory 1', '0a3ffe7c-7395-11', '5', current_timestamp(), current_timestamp(), false),
    ('ACC0004', 'Laptop Accessory 1', '0a3ffe7c-7395-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0005', 'Laptop Accessory 1', '0a3ffe7c-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', 'Manuka Honey UMF5 250g', '0a4000ac-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', 'Manuka Honey UMF5 500g', '0a4000ac-7395-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', 'Manuka Honey UMF5 1kg', '0a4000ac-7395-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', 'Manuka Honey UMF10 250g', '0a4000ac-7395-11', '30', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', 'Manuka Honey UMF10 500g', '0a4000ac-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', 'Manuka Honey UMF10 1kg', '0a4000ac-7395-11', '90', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', 'Manuka Honey UMF20 250g', '0a4000ac-7395-11', '60', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', 'Manuka Honey UMF20 500g', '0a4000ac-7395-11', '100', current_timestamp(), current_timestamp(), false)
    """

cursor.execute(insert_products_statment)

insert_product_prices_statment = \
    """
    insert into product_prices
    (product_code, selling_company_id, price, created_datetime, updated_datetime, is_deleted)
    values
    ('LAPTOP1', '0a3601ca-7395-11', '1500', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '0a3601ca-7395-11', '2000', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '0a3601ca-7395-11', '1000', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '0a3601ca-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '0a3601ca-7395-11', '5', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '0a3601ca-7395-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '0a3601ca-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '0a3601ca-7395-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '0a3601ca-7395-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '0a3601ca-7395-11', '35', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '0a3601ca-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '0a3601ca-7395-11', '40', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '0a3601ca-7395-11', '70', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '0a3601ca-7395-11', '100', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '0a3601ca-7395-11', '80', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '0a3601ca-7395-11', '130', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d1eaf85-7466-11', '1400', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d1eaf85-7466-11', '2100', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d1eaf85-7466-11', '900', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d1eaf85-7466-11', '55', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d1eaf85-7466-11', '5', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d1eaf85-7466-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d1eaf85-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d1eaf85-7466-11', '22', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d1eaf85-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d1eaf85-7466-11', '35', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d1eaf85-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d1eaf85-7466-11', '40', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d1eaf85-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d1eaf85-7466-11', '100', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d1eaf85-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d1eaf85-7466-11', '130', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d200c82-7466-11', '1500', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d200c82-7466-11', '2050', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d200c82-7466-11', '950', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d200c82-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d200c82-7466-11', '5', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d200c82-7466-11', '12', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d200c82-7466-11', '14', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d200c82-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d200c82-7466-11', '18', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d200c82-7466-11', '35', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d200c82-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d200c82-7466-11', '40', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d200c82-7466-11', '78', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d200c82-7466-11', '110', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d200c82-7466-11', '70', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d200c82-7466-11', '130', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d200ed4-7466-11', '1600', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d200ed4-7466-11', '2200', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d200ed4-7466-11', '1000', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d200ed4-7466-11', '50', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d200ed4-7466-11', '5', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d200ed4-7466-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d200ed4-7466-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d200ed4-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d200ed4-7466-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d200ed4-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d200ed4-7466-11', '60', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d200ed4-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d200ed4-7466-11', '80', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d200ed4-7466-11', '140', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d200ed4-7466-11', '80', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d200ed4-7466-11', '140', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '0a3604c0-7395-11', '1800', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '0a3604c0-7395-11', '2400', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '0a3604c0-7395-11', '1400', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '0a3604c0-7395-11', '70', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '0a3604c0-7395-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '0a3604c0-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '0a3604c0-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '0a3604c0-7395-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '0a3604c0-7395-11', '30', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '0a3604c0-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '0a3604c0-7395-11', '80', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '0a3604c0-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '0a3604c0-7395-11', '90', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '0a3604c0-7395-11', '160', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '0a3604c0-7395-11', '90', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '0a3604c0-7395-11', '160', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d20108a-7466-11', '1750', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d20108a-7466-11', '2350', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d20108a-7466-11', '1350', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d20108a-7466-11', '65', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d20108a-7466-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d20108a-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d20108a-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d20108a-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d20108a-7466-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d20108a-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d20108a-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d20108a-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d20108a-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d20108a-7466-11', '155', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d20108a-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d20108a-7466-11', '155', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d2010db-7466-11', '1850', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d2010db-7466-11', '2450', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d2010db-7466-11', '1450', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d2010db-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d2010db-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d2010db-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d2010db-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d2010db-7466-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d2010db-7466-11', '35', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d2010db-7466-11', '55', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d2010db-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d2010db-7466-11', '55', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d2010db-7466-11', '95', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d2010db-7466-11', '165', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d2010db-7466-11', '95', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d2010db-7466-11', '165', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '0a400032-7395-11', '1800', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '0a400032-7395-11', '2400', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '0a400032-7395-11', '1400', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '0a400032-7395-11', '70', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '0a400032-7395-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '0a400032-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '0a400032-7395-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '0a400032-7395-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '0a400032-7395-11', '30', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '0a400032-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '0a400032-7395-11', '80', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '0a400032-7395-11', '50', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '0a400032-7395-11', '90', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '0a400032-7395-11', '160', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '0a400032-7395-11', '90', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '0a400032-7395-11', '160', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d200f5f-7466-11', '1750', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d200f5f-7466-11', '2350', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d200f5f-7466-11', '1350', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d200f5f-7466-11', '65', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d200f5f-7466-11', '10', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d200f5f-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d200f5f-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d200f5f-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d200f5f-7466-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d200f5f-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d200f5f-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d200f5f-7466-11', '45', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d200f5f-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d200f5f-7466-11', '155', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d200f5f-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d200f5f-7466-11', '155', current_timestamp(), current_timestamp(), false),

    ('LAPTOP1', '6d201026-7466-11', '1850', current_timestamp(), current_timestamp(), false),
    ('LAPTOP2', '6d201026-7466-11', '2450', current_timestamp(), current_timestamp(), false),
    ('LAPTOP3', '6d201026-7466-11', '1450', current_timestamp(), current_timestamp(), false),
    ('ACC0001', '6d201026-7466-11', '75', current_timestamp(), current_timestamp(), false),
    ('ACC0002', '6d201026-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0003', '6d201026-7466-11', '15', current_timestamp(), current_timestamp(), false),
    ('ACC0004', '6d201026-7466-11', '20', current_timestamp(), current_timestamp(), false),
    ('ACC0005', '6d201026-7466-11', '25', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-250G', '6d201026-7466-11', '35', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-500G', '6d201026-7466-11', '55', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF5-1KG', '6d201026-7466-11', '85', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-250', '6d201026-7466-11', '55', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-500G', '6d201026-7466-11', '95', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF10-1KG', '6d201026-7466-11', '165', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-250G', '6d201026-7466-11', '95', current_timestamp(), current_timestamp(), false),
    ('MANUKA-UMF20-500G', '6d201026-7466-11', '165', current_timestamp(), current_timestamp(), false)
    """

cursor.execute(insert_product_prices_statment)

### Commit insert statements above
orderdb.commit()

#orderdb.reset_session()
cursor.execute('drop table if exists order_lines')
cursor.execute(
    """
    create table order_lines (
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
        is_deleted boolean
    )
    """
)

cursor.execute('drop table if exists order_headers')
cursor.execute(
    """
    create table order_headers (
        order_no varchar(16),
        order_date date,
        company_id varchar(46),
        customer_document_no varchar(8),
        total decimal(20,4),
        created_datetime datetime,
        updated_datetime datetime,
        is_deleted boolean
    )
    """
)

### Just to populate data randomly first
cursor.execute(
    """
    create table order_lines_temp (
        order_no varchar(16),
        order_line_no varchar(4),
        order_date date,
        company_id binary(16),
        customer_document_no varchar(8),
        product_code varchar(50),
        supplier_id binary(16),
        unit_price decimal(10,2),
        quantity int,
        line_total decimal(20,4),
        created_datetime datetime,
        updated_datetime datetime
    )
    """
)

product_dict_list = {}
cursor.execute(
    """
    select pp.selling_company_id, pp.product_code, pp.price, p.supplier_id
    from product_prices pp
    join products p on pp.product_code = p.product_code
    """
)

for line in cursor:
    company_id = line[0]
    if company_id not in product_dict_list.keys():
        product_dict_list[company_id] = []
    product_dict_list[company_id].append([line[1], line[2], line[3]])

# product_list[0][2]

customer_dict_list = {}
cursor.execute(
    """
    select country, customer_document_no from customers
    """
)

for line in cursor:
    country = line[0]
    if country not in customer_dict_list.keys():
        customer_dict_list[country] = []
    customer_dict_list[country].append(line[1])

# customer_dict_list

selling_companies_list = []
cursor.execute(
    """
    select company_id, country from companies where is_supplier = false
    """
)
for line in cursor:
    selling_companies_list.append([line[0], line[1]])

# selling_companies_list

supplier_companies_list = []
cursor.execute(
    """
    select company_id from companies where is_supplier = true
    """
)
for line in cursor:
    supplier_companies_list.append(line[0])

# supplier_companies_list

import random
import datetime

# product_list # product_code, supplier_id, default_price
# customer_list
# selling_companies_list
# supplier_companies_list

### Populate ORDER LINES data randomly

order_number = 1
order_date = datetime.date(2021,1,1)
count = 1
order_line_insert_statement = ''
# Loop for each ORDER DATE until order date is 13 Jan 2022
while order_date <= datetime.date(2022,1,13):
    order_date_str = order_date.strftime('%Y-%m-%d')
    print('Working on the order date {}'.format(order_date_str))
    # We will have a random number of orders between 1 and 15
    number_of_orders = random.randrange(1,20)
    # Loop for each ORDER
    for o in range(1, number_of_orders+1):
        # We will have a random number of order lines between 1 and 10
        order_number_str = ('0000000000000000' + str(order_number))[-16:]
        number_of_order_lines = random.randrange(1,10)
        random_selling_company = random.choice(selling_companies_list)
            # company_id, country
        random_selling_company_id = random_selling_company[0]
        random_selling_company_country = random_selling_company[1]
        random_customer = random.choice(customer_dict_list[random_selling_company_country])
        for ol in range(1, number_of_order_lines+1):
            ### do something for order line
            order_line_number_str = ('0000' + str(ol))[-4:]
            # Pick a selling company first so that we know which country it is
            random_product = random.choice(product_dict_list[random_selling_company_id])
            random_product_code = random_product[0]
            random_product_price = random_product[1]
            random_product_supplier_id = random_product[2]
            random_quantity = random.randrange(1,21)
            order_line_row_dict = {
                'order_no': order_number_str,
                'order_line_no': order_line_number_str,
                'order_date': order_date_str,
                'company_id': random_selling_company_id,
                'customer_document_no': random_customer,
                'product_code': random_product_code,
                'supplier_id': random_product_supplier_id,
                'unit_price': random_product_price,
                'quantity': random_quantity,
                'line_total': random_product_price * random_quantity,
            }
            if count == 1:
                order_line_insert_statement = \
                    """
                    insert into order_lines_temp
                    (order_no, order_line_no, order_date, company_id, customer_document_no, product_code, supplier_id, unit_price, quantity, line_total, created_datetime, updated_datetime)
                    values"""
            order_line_insert_statement += \
                """
                ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, current_timestamp(), current_timestamp()),""".format(
                    order_line_row_dict['order_no'],
                    order_line_row_dict['order_line_no'],
                    order_line_row_dict['order_date'],
                    order_line_row_dict['company_id'],
                    order_line_row_dict['customer_document_no'],
                    order_line_row_dict['product_code'],
                    order_line_row_dict['supplier_id'],
                    order_line_row_dict['unit_price'],
                    order_line_row_dict['quantity'],
                    order_line_row_dict['line_total'],
                )
            # print(order_line_insert_statement)
            # Commit for every 1000 rows, if not increase the count by 1
            if count == 1000:
                order_line_insert_statement = order_line_insert_statement[:-1]
                #print(order_line_insert_statement)
                cursor.execute(order_line_insert_statement)
                orderdb.commit()
                count = 1
            else:
                count += 1
        order_number += 1
    # Increase Order Date by 1 day
    order_date = order_date + datetime.timedelta(days=1)

order_line_insert_statement = order_line_insert_statement[:-1]
cursor.execute(order_line_insert_statement)
orderdb.commit()

### Populate order_lines
cursor.execute(
    """
    insert into order_lines
    select order_no, order_line_no, order_date, product_code, supplier_id, unit_price, quantity, line_total, current_timestamp(), current_timestamp(), false
    from order_lines_temp
    """
)
orderdb.commit()

### Populate order_headers by aggregating
cursor.execute(
    """
    insert into order_headers
    select order_no, order_date, company_id, customer_document_no, sum(line_total) as total, current_timestamp(), current_timestamp(), false
    from order_lines_temp
    group by order_no, order_date, company_id, customer_document_no
    """
)
orderdb.commit()

### Verify numbers
cursor.execute("select 'order_headers', count(distinct order_no), sum(total) from order_headers")
for line in cursor:
    print(line)
# ('order_headers', 3767, Decimal('73156959.0000'))

cursor.execute("select 'order_lines', count(distinct order_no), sum(line_total) from order_lines")
for line in cursor:
    print(line)
# ('order_lines', 3767, Decimal('73156959.0000'))


"""
cursor.execute('alter table products add is_deleted boolean;')
cursor.execute('update products set is_deleted = false;')
orderdb.commit()

cursor.execute('alter table product_prices add is_deleted boolean;')
cursor.execute('update product_prices set is_deleted = false;')
orderdb.commit()

cursor.execute('alter table order_lines add is_deleted boolean;')
cursor.execute('update order_lines set is_deleted = false;')
orderdb.commit()

cursor.execute('alter table order_headers add is_deleted boolean;')
cursor.execute('update order_headers set is_deleted = false;')
orderdb.commit()

cursor.execute('alter table customers add is_deleted boolean;')
cursor.execute('update customers set is_deleted = false;')
orderdb.commit()

cursor.execute('alter table companies add is_deleted boolean;')
cursor.execute('update companies set is_deleted = false;')
orderdb.commit()
"""
