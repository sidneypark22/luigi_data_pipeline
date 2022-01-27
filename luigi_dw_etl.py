import luigi
import mysql.connector
import boto3
import os
import datetime
import urllib
import json
import re
import redshift_connector

access_key = ''
secret_key = ''

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

s3_client = session.client('s3')

mysql_conn_params = {
    'host': "", # change the host name accordingly
    'user': "",
    'password': "",
    'database': "",
}

redshift_conn_params = {
    'host': '',
    'database': '',
    'port': 5439,
    'user': '',
    'password': ''
}

s3_bucket = ''
s3_bucket_prefix = ''

### Access token required to run ipinfo api - used to look up ip address and find country
ipinfo_token = ''

## array for each table consists of:
## 1) Columns
## 2) Where Clause
## 3) Is Incremental
## 4) Watermark Column
## 5) Corresponding Landing Table
## 6) Corresponding Store Table
## 7) Unique Lookup Columns
tables_to_load_mysql = {
    'order_lines': ['order_no,order_line_no,order_date,product_code,supplier_id,unit_price,quantity,line_total,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_order_lines', 'store_mysql.order_lines', 'order_no,order_line_no'],
    'order_headers': ['order_no,order_date,company_id,customer_document_no,total,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_order_headers', 'store_mysql.order_headers', 'order_no'],
    'companies': ['company_id,company_name,country,is_supplier,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_companies', 'store_mysql.companies', 'company_id'],
    'customers': ['customer_document_no,customer_full_name,username,date_of_birth,address_line_1,address_line_2,address_line_3,city,state,country,post_code,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_customers', 'store_mysql.customers', 'customer_document_no'],
    'products': ['product_code,product_name,supplier_id,default_price,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_products', 'store_mysql.products', 'product_code,supplier_id'],
    'product_prices': ['product_code,selling_company_id,price,created_datetime,updated_datetime,is_deleted', 'where 1=1', False, 'updated_datetime', 'land_source_s3.mysql_product_prices', 'store_mysql.product_prices', 'product_code,selling_company_id'],
}

tables_to_load_weblog = {
    'access': ['ip_address,request_identity,username,request_timestamp,request_line,status_code,response_size,referer,user_agent,country,device,request_timestamp_converted', '', '', '', 'land_source_s3.weblog_access', 'store_weblog.access', 'ip_address,request_timestamp'],
}

tables_to_load_store = {**tables_to_load_mysql, **tables_to_load_weblog}

pres_sql_file_list = os.listdir('./redshift/sql/pres/')

def try_convert_to_int(str_to_convert):
    try:
        return int(str_to_convert)
    except ValueError:
        return None

###############################################
###    Execution Sequence Process Begins    ###
###############################################

#today_str = datetime.datetime.today().strftime('%Y%m%d')
if 'execution_sequence' not in os.listdir():
    os.mkdir('execution_sequence')

existing_execution_sequences = [try_convert_to_int(item) for item in os.listdir('execution_sequence') if try_convert_to_int(item) != None]
existing_execution_sequences.sort(reverse=True)
last_execution_sequence = (lambda x: 0 if len(x) == 0 else x[0])(existing_execution_sequences)
if last_execution_sequence > 0:
    # Last execution was completed successfully, therefore start a new execution sequence
    if 'RunJob' in os.listdir('execution_sequence/{}/output'.format(last_execution_sequence)):
        last_execution_completed = True
        execution_sequence_to_run = last_execution_sequence + 1
    # Last execution was not completed successfully, therefore continue from the last execution sequence
    else:
        last_execution_completed = False
        execution_sequence_to_run = last_execution_sequence
# This is very first initial job execution
else:
    execution_sequence_to_run = 1

execution_sequence_folder = 'execution_sequence/{}'.format(execution_sequence_to_run)
last_execution_sequence_folder = 'execution_sequence/{}'.format(last_execution_sequence)
if str(execution_sequence_to_run) not in os.listdir('execution_sequence'):
    os.mkdir(execution_sequence_folder)
if 'mysql' not in os.listdir(execution_sequence_folder):
    os.mkdir(execution_sequence_folder + '/mysql')
if 'weblog' not in os.listdir(execution_sequence_folder):
    os.mkdir(execution_sequence_folder + '/weblog')
if 'output' not in os.listdir(execution_sequence_folder):
    os.mkdir(execution_sequence_folder + '/output')
if 'completed' not in os.listdir(execution_sequence_folder):
    os.mkdir(execution_sequence_folder + '/completed')
if 'land' not in os.listdir(execution_sequence_folder + '/completed'):
    os.mkdir(execution_sequence_folder + '/completed/land')
if 'store' not in os.listdir(execution_sequence_folder + '/completed'):
    os.mkdir(execution_sequence_folder + '/completed/store')
if 'pres' not in os.listdir(execution_sequence_folder + '/completed'):
    os.mkdir(execution_sequence_folder + '/completed/pres')
if 'watermark' not in os.listdir(execution_sequence_folder):
    os.mkdir(execution_sequence_folder + '/watermark')
if 'start_watermark' not in os.listdir(execution_sequence_folder + '/watermark'):
    os.mkdir(execution_sequence_folder + '/watermark/start_watermark')
if 'end_watermark' not in os.listdir(execution_sequence_folder + '/watermark'):
    os.mkdir(execution_sequence_folder + '/watermark/end_watermark')

## This process is to make sure we don't run 
## the task for the table already completed in case of restart
completed_land_tables = [table for table in os.listdir(execution_sequence_folder + '/completed/land')]
completed_store_tables = [table for table in os.listdir(execution_sequence_folder + '/completed/store')]
completed_pres_sql_files = [table for table in os.listdir(execution_sequence_folder + '/completed/pres')]

for completed_land_table in completed_land_tables:
    tables_to_load_mysql.pop(completed_land_table, None)
for completed_land_table in completed_land_tables:
    tables_to_load_weblog.pop(completed_land_table, None)
for completed_store_table in completed_store_tables:
    tables_to_load_store.pop(completed_store_table, None)
for completed_pres_sql_file in completed_pres_sql_files:
    # pres_sql_file_list is a list therefore use remove to remove by element value
    pres_sql_file_list.remove(completed_pres_sql_file)


#############################################
###    Execution Sequence Process Ends    ###
#############################################




######################################
###    Watermark Process Begins    ###
######################################
# If initial load, then the start watermark is 1900-01-01
start_watermark = ""
if last_execution_sequence == 0:
    start_watermark = datetime.datetime(year=1900,month=1,day=1).strftime('%Y-%m-%d %H:%M:%S')
else:
    if not last_execution_completed:
        if 'watermark' in os.listdir(execution_sequence_folder):
            if 'start_watermark' in os.listdir(execution_sequence_folder + "/watermark"):
                start_watermark_files = os.listdir(execution_sequence_folder + "/watermark/start_watermark")
                if len(start_watermark_files) > 0:
                    start_watermark = start_watermark_files[0]
## if start_watermark is blank, it means the last execution was successful 
## or the current execution sequence failed before creating start_watermark file
if start_watermark == "":
    # This means it is the intial load, but the error caused the start watermark to be not created
    if execution_sequence_to_run == 1:
        start_watermark = datetime.datetime(year=1900,month=1,day=1).strftime('%Y-%m-%d %H:%M:%S')
    # Start watermark is the end watermark of the last execution sequence
    else:
        last_end_watermark_files = os.listdir(last_execution_sequence_folder + "/watermark/end_watermark")
        start_watermark = last_end_watermark_files[0]

## check if end_watermark already exists. 
## If it does, it means the current sequence is restarted and we should stick with that.
end_watermark = ""
if 'watermark' in os.listdir(execution_sequence_folder):
    if 'end_watermark' in os.listdir(execution_sequence_folder + "/watermark"):
            end_watermark_files = os.listdir(execution_sequence_folder + "/watermark/end_watermark")
            if len(end_watermark_files) > 0:
                end_watermark = end_watermark_files[0]
## If there is no watermark exists, we will set the new end_watermark now
if end_watermark == "":
    end_watermark = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

## Create watermark files
if len(os.listdir(execution_sequence_folder + "/watermark/start_watermark")) == 0:
    open(execution_sequence_folder + "/watermark/start_watermark/" + start_watermark, 'w').close()
if len(os.listdir(execution_sequence_folder + "/watermark/end_watermark")) == 0:
    open(execution_sequence_folder + "/watermark/end_watermark/" + end_watermark, 'w').close()

####################################
###    Watermark Process Ends    ###
####################################



#######################################
###    Reusable Functions Begins    ###
#######################################

def extract_from_mysql_tables_to_s3(table_name, table_dict, execution_sequence_folder):
    table_columns = table_dict[0]
    where_clause = table_dict[1]
    is_incremental = table_dict[2]
    watermark_column = table_dict[3]
    orderdb = mysql.connector.connect(
        host=mysql_conn_params['host'],
        user=mysql_conn_params['user'],
        password=mysql_conn_params['password'],
        database=mysql_conn_params['database']
    )
    if is_incremental:
        watermark_filter = " and {} >= '{}' and {} < '{}'".format(watermark_column, start_watermark, watermark_column, end_watermark)
    else:
        watermark_filter = ""
    mysql_cursor = orderdb.cursor(buffered=True)
    mysql_cursor.execute("""
    select {table_columns}
    from {table_name}
    {where_clause}{watermark_filter}
    """.format(table_name=table_name, table_columns=table_columns, where_clause=where_clause, watermark_filter=watermark_filter)
    )
    dump_file_name = '{execution_sequence_folder}/mysql/{table_name}.csv'.format(execution_sequence_folder=execution_sequence_folder, table_name=table_name)
    s3_file_name = 'mysql/{table_name}/{execution_sequence_folder}/{file_name}.csv'.format(table_name=table_name, execution_sequence_folder=execution_sequence_folder, file_name=table_name)
    dump_file = open(dump_file_name, 'w')
    for line in mysql_cursor:
        dump_file.write(",".join('"'+str(element)+'"' for element in line) + '\n')
    dump_file.close()
    s3_client.upload_file(
        Filename=dump_file_name,
        Bucket=s3_bucket,
        Key='{s3_bucket_prefix}{s3_file_name}'.format(s3_bucket_prefix=s3_bucket_prefix, s3_file_name=s3_file_name),
    )


def try_convert_web_log_timestamp(weblog_timestamp):
    # weblog_timestamp example: 31/Jul/2021:23:55:26 +0000
    try:
        return datetime.datetime.strptime(weblog_timestamp[:-6], '%d/%b/%Y:%H:%M:%S')
    except ValueError:
        return datetime.datetime(year=1900, month=1, day=1)
    

def get_country_from_ip_address(ip_address, ipinfo_token):
    token = ipinfo_token
    request_url = 'https://ipinfo.io/{ip_address}\?token={token}'.format(ip_address=ip_address, token=token)
    response = urllib.request.urlopen(request_url)
    ip_lookup_result = response.read()
    country = json.loads(ip_lookup_result)['country']
    return country


def prepare_store_table_update_script(table_name, table_dict):
    table_columns = table_dict[0]
    where_clause = table_dict[1]
    is_incremental = table_dict[2]
    watermark_column = table_dict[3]
    land_table = table_dict[4]
    store_table = table_dict[5]
    lookup_column = table_dict[6]
    sql_statements_list = []
    update_column_list = table_columns
    update_land_table = land_table
    update_execution_sequence = execution_sequence_to_run
    update_store_table_1 = store_table
    update_set_column_mapping = ",\n".join(['{} = landing.{}'.format(col, col) for col in table_columns.split(',')])
    update_store_table_2 = store_table
    update_lookup_column_mapping = " and \n".join(['target.{} = landing.{}'.format(col, col) for col in lookup_column.split(',')])
    store_update_statement = """with cte_landing as (
    select {update_column_list}
    from {update_land_table}
    where execution_sequence = {update_execution_sequence}
    )
    update {update_store_table_1}
    set
        {update_set_column_mapping},
        etl_updated_datetime = date_trunc('second', cast(current_timestamp as datetime))
    from cte_landing landing
    join {update_store_table_2} target
    on {update_lookup_column_mapping};""".format(
        update_column_list=update_column_list,
        update_land_table=update_land_table,
        update_execution_sequence=update_execution_sequence,
        update_store_table_1=update_store_table_1,
        update_set_column_mapping=update_set_column_mapping,
        update_store_table_2=update_store_table_2,
        update_lookup_column_mapping=update_lookup_column_mapping
    )
    sql_statements_list.append(store_update_statement)
    insert_store_table_1 = store_table
    insert_column_list = table_columns
    insert_land_table = land_table
    insert_execution_sequence = execution_sequence_to_run
    insert_store_table_2 = store_table
    insert_lookup_column_mapping = " and \n".join(['target.{} = landing.{}'.format(col, col) for col in lookup_column.split(',')])
    store_insert_statement = """insert into {insert_store_table_1}
    with cte_landing as (
    select {insert_column_list}
    from {insert_land_table}
    where execution_sequence = {insert_execution_sequence}
    )
    select landing.*
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
    , date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
    from cte_landing landing
    left join {insert_store_table_2} target
    on {insert_lookup_column_mapping}
    where target.etl_updated_datetime is null;""".format(
        insert_store_table_1=insert_store_table_1,
        insert_column_list=insert_column_list,
        insert_land_table=insert_land_table,
        insert_execution_sequence=insert_execution_sequence,
        insert_store_table_2=insert_store_table_2,
        insert_lookup_column_mapping=insert_lookup_column_mapping,
    )
    sql_statements_list.append(store_insert_statement)
    return sql_statements_list

"""
def run_script_on_redshift(redshift_conn, script):
    cursor = redshift_conn.cursor()
    cursor.execute(script)
""" 

#######################################
###    Reusable Functions Ends    ###
#######################################



################################
###    Luigi Tasks Begins    ###
################################

class StartJob(luigi.Task):
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))

class StartExtract(luigi.Task):
    def requires(self):
        return [StartJob()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class ExtractMysqlTablesToS3(luigi.Task):
    def requires(self):
        return [StartExtract()]
    def run(self):
        class_name = self.__class__.__name__
        tables_to_load = tables_to_load_mysql.copy()
        redshift_conn = redshift_connector.connect(
                host=redshift_conn_params['host'],
                database=redshift_conn_params['database'],
                port=redshift_conn_params['port'],
                user=redshift_conn_params['user'],
                password=redshift_conn_params['password']
        )
        redshift_conn.autocommit = True
        for table in tables_to_load.keys():
            table_name = table
            table_columns = tables_to_load[table][0]
            where_clause = tables_to_load[table][1]
            is_incremental = tables_to_load[table][2]
            watermark_column = tables_to_load[table][3]
            land_table = tables_to_load[table][4]
            store_table = tables_to_load[table][5]
            lookup_column = tables_to_load[table][6]
            extract_from_mysql_tables_to_s3(table_name, tables_to_load[table], execution_sequence_folder)
            partition_add_script = """alter table {land_table} 
            add if not exists partition(execution_sequence={execution_sequence_1})
            location 's3://dw-dev-source/mysql/{table_name}/execution_sequence/{execution_sequence_1}/';""".format(
                land_table=land_table,
                table_name=table_name,
                execution_sequence_1=execution_sequence_to_run,
                execution_sequence_2=execution_sequence_to_run,
            )
            cursor = redshift_conn.cursor()
            cursor.execute(partition_add_script)
            open('./{execution_sequence_folder}/completed/land/{table_name}'.format(execution_sequence_folder=execution_sequence_folder, table_name=table_name), 'w').close()
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class ExtractWeblogToS3(luigi.Task):
    def requires(self):
        return [StartExtract()]
    def run(self):
        class_name = self.__class__.__name__
        tables_to_load = tables_to_load_weblog.copy()
        redshift_conn = redshift_connector.connect(
                host=redshift_conn_params['host'],
                database=redshift_conn_params['database'],
                port=redshift_conn_params['port'],
                user=redshift_conn_params['user'],
                password=redshift_conn_params['password']
        )
        redshift_conn.autocommit = True
        for table in tables_to_load.keys():
            table_name = table
            land_table = tables_to_load[table][4]
            pattern = re.compile(r'''(?:^| )(\"(?:[^\"]+|\"\")*\"|[^ ]*)''')
            ip_address_list = set()
            with open('{table_name}.log'.format(table_name=table_name), 'r') as f:
                file_content = f.readlines()
            file_delimited = open('{execution_sequence_folder}/weblog/{table_name}_delimited.log'.format(
                execution_sequence_folder=execution_sequence_folder,
                table_name=table_name
            ), 'w')
            for line in file_content:
                line_split = pattern.split(line.replace('[', '"').replace(']', '"'))
                empty_string_idx = []
                for idx in range(len(line_split)):
                    if line_split[idx] == '':
                        empty_string_idx.append(idx)
                new_line = []
                for idx in range(len(line_split)):
                    if idx not in empty_string_idx:
                        new_line.append(('"' + line_split[idx] + '"').replace('""', '"'))
                new_line = new_line[:-1]
                ip_address_list.add(new_line[0])
                file_delimited.write(",".join(new_line) + '\n')
            file_delimited.close()
            ip_address_country = {}
            for ip_address_quote in ip_address_list:
                ip_address = ip_address_quote[1:-1]
                ip_address_country[ip_address] = get_country_from_ip_address(ip_address, ipinfo_token)
            #print(ip_address_country)
            with open('{execution_sequence_folder}/weblog/{table_name}_delimited.log'.format(
                execution_sequence_folder=execution_sequence_folder,
                table_name=table_name
            ), 'r') as f:
                file_content = f.readlines()
            pattern = re.compile(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")
            file_delimited_extra = open('{execution_sequence_folder}/weblog/{table_name}_delimited_extra.log'.format(
                execution_sequence_folder=execution_sequence_folder,
                table_name=table_name
            ), 'w')
            for line in file_content:
                line_split = pattern.split(line.replace('\n',''))
                country_lookup = ip_address_country[line_split[0][1:-1]]
                device_lookup = ""
                for device_to_search in ['Android', 'iPhone', 'Windows', 'Macintosh', 'iPad']:
                    if line_split[8].find(device_to_search) > -1:
                        device_lookup = device_to_search
                        break
                device_lookup = (lambda x: "Other" if x == "" else x)(device_lookup)
                timestamp_converted = try_convert_web_log_timestamp(line_split[3][1:-1]).strftime('%Y-%m-%d %H:%M:%S')
                line_split.append('"' + country_lookup + '"')
                line_split.append('"' + device_lookup + '"')
                line_split.append('"' + timestamp_converted + '"')
                file_delimited_extra.write(",".join(line_split) + '\n')
            file_delimited_extra.close()
            dump_file_name = '{execution_sequence_folder}/weblog/{table_name}_delimited_extra.log'.format(execution_sequence_folder=execution_sequence_folder, table_name=table_name)
            s3_file_name = 'weblog/{table_name}/{execution_sequence_folder}/{file_name}.csv'.format(table_name=table_name, execution_sequence_folder=execution_sequence_folder, file_name=table_name)
            s3_client.upload_file(
                Filename=dump_file_name,
                Bucket=s3_bucket,
                Key='{s3_bucket_prefix}{s3_file_name}'.format(s3_bucket_prefix=s3_bucket_prefix, s3_file_name=s3_file_name),
            )
            partition_add_script = """alter table {land_table} 
            add if not exists partition(execution_sequence={execution_sequence_1})
            location 's3://dw-dev-source/weblog/{table_name}/execution_sequence/{execution_sequence_1}/';""".format(
                land_table=land_table,
                table_name=table_name,
                execution_sequence_1=execution_sequence_to_run,
                execution_sequence_2=execution_sequence_to_run,
            )
            cursor = redshift_conn.cursor()
            cursor.execute(partition_add_script)
            open('./{execution_sequence_folder}/completed/land/{table_name}'.format(execution_sequence_folder=execution_sequence_folder, table_name=table_name), 'w').close()
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))



class EndExtract(luigi.Task):
    def requires(self):
        return [ExtractMysqlTablesToS3(), ExtractWeblogToS3()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))

class StartStoreUpdate(luigi.Task):
    def requires(self):
        return [EndExtract()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))

class UpdateStoreTables(luigi.Task):
    def requires(self):
        return [StartStoreUpdate()]
    def run(self):
        class_name = self.__class__.__name__
        redshift_conn = redshift_connector.connect(
            host=redshift_conn_params['host'],
            database=redshift_conn_params['database'],
            port=redshift_conn_params['port'],
            user=redshift_conn_params['user'],
            password=redshift_conn_params['password']
        )
        redshift_conn.autocommit = True
        for key in tables_to_load_store.keys():
            table_name = key
            table_dict = tables_to_load_store[key]
            sql_statements_list = prepare_store_table_update_script(table_name, table_dict)
            for sql_statement in sql_statements_list:
                cursor = redshift_conn.cursor()
                cursor.execute(sql_statement)
                open('./{execution_sequence_folder}/completed/store/{table_name}'.format(execution_sequence_folder=execution_sequence_folder, table_name=table_name), 'w').close()
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class EndStoreUpdate(luigi.Task):
    def requires(self):
        return [UpdateStoreTables()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))

class StartPresUpdate(luigi.Task):
    def requires(self):
        return [EndStoreUpdate()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))



class UpdatePresTables(luigi.Task):
    def requires(self):
        return [StartPresUpdate()]
    def run(self):
        class_name = self.__class__.__name__
        redshift_conn = redshift_connector.connect(
            host=redshift_conn_params['host'],
            database=redshift_conn_params['database'],
            port=redshift_conn_params['port'],
            user=redshift_conn_params['user'],
            password=redshift_conn_params['password']
        )
        redshift_conn.autocommit = True
        for sql_file in pres_sql_file_list:
            with open('./redshift/sql/pres/{}'.format(sql_file)) as f:
                sql_statements = f.read()
            sql_statements_list = sql_statements.split("--statement_end")[:-1]
            for sql_statement in sql_statements_list:
                cursor = redshift_conn.cursor()
                cursor.execute(sql_statement)
            open('./{execution_sequence_folder}/completed/pres/{sql_file}'.format(execution_sequence_folder=execution_sequence_folder, sql_file=sql_file), 'w').close()
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class EndPresUpdate(luigi.Task):
    def requires(self):
        return [UpdatePresTables()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class EndJob(luigi.Task):
    def requires(self):
        return [EndPresUpdate()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


class RunJob(luigi.Task):
    def requires(self):
        return [EndJob()]
    def run(self):
        class_name = self.__class__.__name__
        with self.output().open('w') as outfile:
            outfile.write("Finished {class_name} task".format(class_name=class_name))
    def output(self):
        class_name = self.__class__.__name__
        return luigi.LocalTarget("./{execution_sequence_folder}/output/{class_name}".format(execution_sequence_folder=execution_sequence_folder, class_name=class_name))


"""
### Update and Insert Examples

sql_statements_list = sql_statements.split('--statement_end') # For dims and facts only

#print(sql_statements_list[0])
for sql_statement in sql_statements_list:
    cursor = conn.cursor()
    cursor.execute(sql_statement)
sql_statements_list[2]
"""


##############################
###    Luigi Tasks Ends    ###
##############################

# /Volumes/GoogleDrive/My Drive/toptal/etl/extract/mysql_to_s3.py
# luigi --module mysql_to_s3 ExtractMysqlTables --local-scheduler --log-level INFO
# export PYTHONPATH=$PYTHONPATH:"/Volumes/GoogleDrive/My Drive/toptal/etl/extract/"
# luigi --module mysql_to_s3 ExtractMysqlTablesToS3 --local-scheduler
# luigi --module mysql_to_s3 StartJob --local-scheduler
# luigi RunJob --module mysql_to_s3 --local-scheduler
# luigi ExtractWeblogToS3 --module mysql_to_s3 --local-scheduler
# luigi RunJob --module luigi_dw_etl --local-scheduler
# luigi RunJob --module luigi_dw_etl --local-scheduler

# export PYTHONPATH=$PYTHONPATH:"/Users/Sidney/GitLab/Toptal/Sidney-Park/etl/extract/"


