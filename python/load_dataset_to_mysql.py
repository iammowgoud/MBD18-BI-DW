# FIRST install MySQL Connector library for Python by running this in the command line/terminal
# pip install mysql-connector

# Import libraries
import mysql.connector
import json
import math

######################
###################### Connection
######################

# Connect to database - CHANGE THIS TO YOUR LOCAL CREDENTIALS
db_connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=""
)

# This is like the SQL command line - we excute SQL queries using it
db_cursor = db_connection.cursor()

######################
###################### Datasets
######################

print("=> Opening dataset 1..")
wells_dataset = json.loads(open("wells_mexico_all.json").read())  # Must be in the same folder as load.py
print("=> dataset 1 opened")

print("=> Opening dataset 2..")
production_dataset = json.loads(open("production_mexico_all.json").read())  # Must be in the same folder as load.py
print("=> dataset 2 opened")

######################
###################### Database
######################

db_cursor.execute("DROP DATABASE IF EXISTS OIL_GAS_PRODUCTION ")
db_cursor.execute("CREATE DATABASE OIL_GAS_PRODUCTION")
db_cursor.execute("USE OIL_GAS_PRODUCTION")

######################
###################### Tables
######################

# db_cursor.execute('CREATE TABLE PRODUCTION (id INT NOT NULL AUTO_INCREMENT, api_number INT NOT NULL,  month VARCHAR(45) NULL,  oil INT NULL,  gas INT NULL,  water INT NULL,  PRIMARY KEY (id))')
# db_cursor.execute('CREATE TABLE WELLS ( id INT NOT NULL AUTO_INCREMENT,  api_number INT NOT NULL,  operator_name VARCHAR(45) NULL,  well_name VARCHAR(45) NULL,  latitude FLOAT NULL,  longitude FLOAT NULL,  status VARCHAR(1) NULL,  parent VARCHAR(45) NULL,  cik INT NULL,  PRIMARY KEY (id));')

######################
###################### Wells Table Insertion
######################

print("=> Inserting Wells")
total_wells = len(wells_dataset['rows'])
wells_row_num = 0

for row in wells_dataset['rows']:
    api_number = row['api_number']
    operator_name = row['operator_name'].replace("'", "''") if row['operator_name']!="" else 'NULL'
    well_name = row['well_name'].replace("'", "''") if row['well_name']!="" else 'NULL'
    latitude = row['latitude'] if row['latitude']!="" else 'NULL'
    longitude = row['longitude'] if row['longitude']!="" else 'NULL'
    status = row['status'].replace("'", "''") if row['status']!="" else 'NULL'
    parent = row['parent'].replace("'", "''") if row['parent']!="" else 'NULL'
    cik = row['cik'] if row['cik']!="" else 'NULL'
    db_cursor.execute(f'INSERT INTO WELLS (api_number ,  operator_name , well_name , latitude ,  longitude , status , parent , cik) VALUES({api_number} ,  \'{operator_name}\' , \'{well_name}\' , {latitude} ,  {longitude} , \'{status}\', \'{parent}\', {cik} ) ')
    wells_row_num = wells_row_num + 1
    print(f'WELLS {wells_row_num} / {total_wells} ===> {math.floor(wells_row_num/total_wells*100)} % ')

db_cursor.execute('COMMIT;')


######################
###################### Production Table Insertion
######################

print("=> Inserting Production (will take a while)")
total_prod = len(production_dataset['rows'])
prod_row_num = 0

for row in production_dataset['rows']:
    api_number = row['api_number']
    month = row['month'].replace("'", "''") if row['month']!="" else 'NULL'
    oil = row['gas'] if row['gas']!="" else 'NULL'
    gas = row['oil'] if row['oil']!="" else 'NULL'
    water = row['water'] if row['water']!="" else 'NULL'
    db_cursor.execute(f'INSERT INTO PRODUCTION  (api_number, month, oil, gas, water) VALUES({api_number}, {month}, {oil}, {gas}, {water})')
    prod_row_num = prod_row_num+1
    print(f' PRODUCTION {prod_row_num} / {total_prod} ===> {math.floor(prod_row_num/total_prod*100)} % ')

db_cursor.execute('COMMIT;')

print('DONE \o\ /o/ \o\ /o/ ')
