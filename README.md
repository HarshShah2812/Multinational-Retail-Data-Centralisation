# Multinational Retail Data Centralisation

This project involves being part of a multinational organisation whose data are spread across a range of sources, with the objective being extracting the data, cleaning it using the Pandas library and uploading it to a PostgreSQL database for querying. 

## Summary

Following the ETL (Extract, Transform, Load) principle, data is collected from various sources, cleaned and sent to a database. Tis process will be performed using three separate scripts available within this repo: database_utils.py, data_extraction.py and data_cleaning.py. The former will connect to an AWS RDS database, the second will extract the data from various sources (RDS tables, API's, S3 bucket etc.), and the latter will clean the data and load it to a database. Once uploaded to the database, the date types for each column will be changed to the correct types in order for the database to be fit for querying. 

6 different tables will be extracted and cleaned in this:
    
    The users table;
    the card details table;
    the stores table;
    the products table;
    the orders table;
    the date table.

## Data Extraction
Here, we will be extracting data from various sources with the data_extraction.py script.

### AWS RDS
Firstly, we connect to the AWS RDS database using SQLAlchemy in the database_utils.py file. Below you can see the code which shows the DatabaseConnector class from the database_utils.py file, containing the `init_db_engine` method which initialises the RDS engine, using login credentials stored in a yaml file. The `list_db_tables` method lists all the tables that are present in the RDS database that we are trying to access.

`

import pandas as pd
from sqlalchemy import create_engine 
from sqlalchemy import inspect
import yaml
import psycopg2
from credentials import credentials


class DatabaseConnector:

    def __init__(self):
        self.connection = psycopg2.connect(host = credentials.get('host'), database = credentials.get('database'), user = credentials.get('user'), password = credentials.get('password'))

    def read_db_creds(self):
       with open ('db_creds.yaml', 'r') as creds:
        creds_loaded = yaml.safe_load(creds)
        return creds_loaded

    def init_db_engine(self):
        creds_loaded = self.read_db_creds()
        engine = create_engine(f"{creds_loaded['DATABASE_TYPE']}+{creds_loaded['DBAPI']}://{creds_loaded['RDS_USER']}:{creds_loaded['RDS_PASSWORD']}@{creds_loaded['RDS_HOST']}:{creds_loaded['RDS_PORT']}/{creds_loaded['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        list_tables = inspector.get_table_names()
        print(list_tables)
        return list_tables

    def upload_to_db(self, dataframe, table_name):
        conn_string = f"postgresql://{credentials.get('user')}:{credentials.get('password')}@{credentials.get('host')}/{credentials.get('database')}"
        db = create_engine(conn_string)
        conn = db.connect()
        conn1 = self.connection
        conn1.autocommit = True
        df = pd.DataFrame(dataframe)
        df_sql = df.to_sql(table_name, conn, if_exists = 'replace')
        return df_sql

`
