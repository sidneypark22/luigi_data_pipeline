import boto3
import time
# 285958899685
access_key = 'your_access_key'
secret_key = 'your_secret_key'

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

rds_client = session.client('rds', 'us-east-1')

#session.client('sts').get_caller_identity()

### Create a Mysql database instance
rds_client.create_db_instance(
    #DBName='order-mgmt-db-2',
    DBInstanceIdentifier='order-mgmt-db',
    AllocatedStorage=20,
    DBInstanceClass='db.t2.micro', # free tier for mysql rds db
    Engine='mysql',
    MasterUsername='',
    MasterUserPassword='',
    MultiAZ=False,
)

### Check the db instance status
while True:
    time.sleep(120)
    db = db_instance['DBInstances'][0]
    db_instance_status = db['DBInstanceStatus']
    db_instance_id = db['DBInstanceIdentifier']
    if db_instance_status == 'available':
        print('Database {database} has been successfully created.'.format(database=db_instance_id))
        break
    elif db_instance_status == 'creating':
        print('Database is still being created.')
        continue
    else:
        print('Please check the database status.')
        break

import mysql.connector

host="" # This host name should be changed accordingly
user=""
password=""
database=""

orderdb = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
)

cursor = orderdb.cursor()

cursor.execute('show databases;')

for x in cursor:
    print(x)

### Create a new database
cursor.execute('create database {database};'.format(database=database))

orderdb.close()