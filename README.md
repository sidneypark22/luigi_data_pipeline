# Luigi Python Data Pipeline Practice

## Sources

### Order Source Database

AWS Mysql RDS database has been created to represent a B2B order management database as AWS provides the Mysql database at a free tier. The database was created on console, but **create_mysql_rds_db.py** file can also be used to create one.

These are a list of tables that have been created to reflect the B2B order management system:

- companies: Contains company data. Each row is unique on company_id. is_supplier can be used to tell if this company is a supplier.
- customers: Contains customer data. ach row is unique on customer_document_no.
- products: Contains product data. Each row is unique on product_code and supplier_id (supplier id is an equivalent to company_id).
- product_prices: Contains product price data. Each row is unique on product_code and company_id. Price for each product_code can be different for each company_id.
- order_headers: Contains order header data. Each row is unique on order_no.
- order_lines: Contains order line data. Each row is unique on order_no and order_line_no.

All the tables above are assumed to have created_datetime, updated_datetime and is_deleted columns in common.

The file **populate_order_db_data.py** contains the scripts that were used to populate the sample data.

### Weblog Access

The file **populate_web_log_data.py** contains the scripts that were used to populate the sample weblog data. The username has been randomly chosen from the **customers** table and the IP address samples have been taken for the countries from the Mysql sample data.


## ELT

### Database

A datawarehouse database has been created on AWS Redshift (initial 2 months on free tier fortunately). The file **redshift/create_database_tables.sql** contains the scripts used to create database, schemas and tables.

There are 3 distinct layers:
- landing: Represents data extracted and loaded from the source.
- store: This is an exact copy of the landing source data. However, it is updated/inserted using the unique columns on the table, which is particularly useful if source data is extracted and loaded incrementally. In the real world, tables in the data store layer can be exposed to analysts who have a good understanding of the company's data model for more flexible ad-hoc type of queries.
- pres: Presentation layer includes data transformed and readily available for reporting for general audience. It is particularly useful to design the presentation layer in star schema, but it has been chosen to present data in the tabular flat structure due to a time constraint.


### Extract

#### Mysql

Data is extracted from Mysql using the Python mysql connector.

Rather than having a sql for each table extract, I have chosen to generate an extract sql query dynamically driven by the metadata defined in the Python dictionaries (It would have been also useful to do this in the database table, but Pythonic way was useful for me due to the time constraint).

In the Python dictionary, the key is the extract table name and the value is an array that consists of:
1. Columns
2. Where Clause
3. Is Incremental
4. Watermark Column
5. Corresponding Landing Table
6. Corresponding Store Table
7. Unique Lookup Columns

Then the same function will be used to dynamically generate the sql queries based on values supplied.

For each execution, it will determine the start and end watermarks which can be used to extract data incrementally if needed.

Once the data is extracted, I have chosen to upload those extract files to AWS S3. Landing tables created in this test project are all external tables partitioned by execution sequence.

Once data is extracted into a file and uploaded to S3, the process will create a new partition for the new execution sequence. In this way, we can later use a specific partition to use what is loaded in the current execution to merge to **store** tables.

#### Web log

The file is present in the folder as **access.log**.

There are a few things done, to be able to utilise Python.
- Convert each column to be enclosed by double quotation marks and delimited by a comma. This is so that it is easier for Redshift to read using csv format
- Lookup IP address and derive a country. IPinfo provides a free of charge api up to 50,000 requests per month. For each row in the file, it will get the IP address then create a distinct list of the IP address present in the file. Then we will use IPinfo api to send the request and get the country from the response (I was hoping that it could reduce the number of requests to be made leading to a cost reduction potentially). Once the country is obtained, then add it as another field in the new file.
- Read the user-agent string and find out a device used to log in. user-agent string samples are taken by accessing whatsmyua.info by using different personal devices. Therefore it will be one of Android, iPhone, Windows, Macintosh or iPad. Once the device is obtained, a new field will be added. By the logic, if it is not one of these, then the device will be Other.
- Try converting the request timestamp into a database standard datetime string i.e. yyyy-mm-dd hh24:mi:ss. Then this is added as a new field named request_timestamp_converted. If it can't be converted, then the value will be 1900-01-01 00:00:00.

Once these transformations are applied, then the file will be loaded to S3, then follow the same process as the Mysql extract.


### Store

The purpose of the store is to store all the source data loaded so far, but keeping each row unique i.e. no duplicates, so that the tables can be used for ad-hoc type of reporting. Also it can be served as a base readily available to rebuild the presentation layer tables.

Since there is a none to minimal transformation is expected in this layer, a process to dynamically generate upsert and insert statements (basically merge - redshift doesn't have a merge statement) based on the table data dictionary supplied at data extract.


### Pres

Data in the presentation layer is transformed and will be used by a general audience to answer business questions. In this test project, reporting data is presented in a tabular flattened structure.

Since there are complicated transformations expected, a script is expected to be provided and placed in **redshift/sql/pres.sql** folder. The ELT process will generate a list of files in that folder during the process, then loop through the list and execute in Redshift.

2 tables are presented in this test project as below:
- pres.report_order_lines: This is the order line data, but the relevant fields from different tables are all consolidated into a single table.
- pres.report_weblog_access: This is the weblog access data, but not much transformations have been applied. However, this data is critical to answer the reporting questions as part of the requirements and need to be presented for a reporting purpose.

Although it is not built within the Luigi data pipeline, scripts to create the dimension and fact tables have been repared for modelling on fact_order_line as well as the update scripts, which are placed under redshift folder. These scripts were created within a short timeframe i.e. 6-8 hours therefore they are very simple but they should be at least able to demonstrate how the proper data warehouse design would have been if dimension modelling was taken in consideration.

All the dimension tables are designed in SCD1 update except for the product where it was designed in SCD2 update to mainly capture changes in default price i.e. cost price as at time of an order placed.

### ETL tool

I would have used a SSIS for a convenience, but I am now using Mac with the new company I've recently started and SQL Server stacks are not very familiar with Mac as you know. Also, since I am using the AWS products, I thought SQL Server stacks don't feel well coupled with them (SQL Server stacks are great and I believe they will be able to perform all the transformation tasks if used correctly though). Therefore I've decided to use this opportunity to get familiar with a new tool. After some searches, Luigi came to my radar. Luigi is similar to Airflow and it was developed by Spotify whereas Airflow was developed by Airbnb. As I like Airflow, I wanted to see what Luigi looks like.

The file **luigi_dw_etl.py** contains all the necessary python scripts to run the ELT for the layers mentioned above. In this script, I've chosen to separate each complete execution by execution sequence. It will start from 1 then increase by 1 after each succesfully completed execution.

Where it uses the loop functionalities, a file will be created for each succesful table update under "completed" folder. What's been marked as completed under this folder will enable the ETL to skip the table update as it will be unnecessary. If a user wants to rerun the table again as part of rerun, then they can delete the file from the ouput folder.

By its default design, Luigi creates an output object when each task is completed, which is used to acknowledge a succesful completion. I have set the output files to be created under the output folder. Output file can be deleted to rerun the task again if needed.

Luigi has a web scheduler interface just like Airflow. One can decides to run a job either locally or via a central scheduler. If it is run via the scheduler, then the progress can be monitored in the web interface. If a task fails, then the error can be found from the web interface - **Luigi Error.png**. If run locally, then the terminal will reveal the error to be investigated.

#### Restarability

In this test project solution built, two tracking files are used to mark the failure point:
- output files: this is to know what tasks have completed so far
- completed files: this is to know what table updates have completed so far in case a loop function is used as nested in a task.
When a job fails, all needs to be done is to simply run an execute command again. Then the Luigi ELT script automatically picks up from where it needs to be restarted by skipping all the tasks and table updates that have already completed.

#### Ability to track

As we have output files and completed files, it can be used to track when each task has been completed.

Luigi web interface also provides when each task has completed/failed. **Luigi Task Timestamps.png**

#### Luigi Job Executions

If run via the central scheduler: 
  luigi RunJob --module luigi_dw_etl
If run locally: 
  luigi RunJob --module luigi_dw_etl --local-scheduler


### Ability to transform larger data size

Redshift can be easily scaled when needed. Therefore, schedules can be created to scale the cluster up and down when a large amount of data is expected to be processed. For example, if the nightly batch ELT process is scheduled at 2AM and expected to take around 4 hours, then one schedule can be created to scale up at, say 1:50AM then the other schedule to scale down at, say 6:30AM. Utilising such options as Lambda, custom Python scripts or Cloudwatch will provide a more flelxibe way to handle scaling option.

Luigi also can run tasks in parallel with an increased number of workers. Therefore, if Luigi is run on a more scalable instance, then the Luigi ELT job can be triggered with more workers to enable task executions in parallel. In such a case, a change will need to be applied to the current ELT solution by splitting the looping function into multiple tasks.

## Reporting questions in the requirements

I have created 3 sql files to answer these questions:
- sql_monthly_sales.sql
- sql_most_popular_products_from_country_with_most_logins.sql
- sql_top_5_most_used_devices.sql
