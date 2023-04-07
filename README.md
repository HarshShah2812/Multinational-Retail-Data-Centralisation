# Multinational Retail Data Centralisation

> This project involves being part of a multinational organisation whose data are spread across a range of sources, with the objective being extracting the data, cleaning it using the Pandas library and uploading it to a PostgreSQL database for querying. 

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

> Here, we will be extracting data from various sources with the data_extraction.py script.

### AWS RDS

Firstly, we connect to the AWS RDS database using SQLAlchemy in the database_utils.py file. Below you can see the code which shows the DatabaseConnector class from the database_utils.py file, containing the `init_db_engine` method which initialises the RDS engine, using login credentials stored in a yaml file. The `list_db_tables` method lists all the tables that are present in the RDS database that we are trying to access.

```python
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
```
In the data_extraction.py file, we create the `DataExtractor` class and the RDS table is read, with the help of SQLAlchemy, using the `read_rds_table` method, which will be used in the data_cleaning.py script.

```python
import pandas as pd
from database_utils import DatabaseConnector
import requests
import tabula
import boto3

class DataExtractor:
    
    def __init__(self):
        self.connector = DatabaseConnector()
       
    def read_rds_table(self, table_name):
        engine = self.connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col = 'index')
        return df
```
### Extract from PDF

The next set of data to extract are card details corresponding to the payments made for each order, which are stored inside a PDF file. The extraction will take place with the help of the Tabula library. As shown below, Tabula reads the PDF, which is then converted to a Pandas DataFrame. The head of the dataframe is then printed to ensure that the data has been extracted correctly.

```python
 def retrieve_pdf_data(self, link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        tab = tabula.read_pdf(link, pages = "all")
        df = pd.DataFrame(tab[0])
        print(df.head())
        return df
```

### Extract from an API

Now, we need need to extract the data for all the stores, which are found in an API(Application Programming Interface). Therefore, we use the Requests library to do this with the `requests.get` command, including headers, which in this case are the API keys necessary for accessing the API. We create 2 methods: `list_number_of_stores`, which returns the number of existing stores, and `retrieve_stores_data`, which returns the data for each of those stores with the help of a 'for' loop, within which the data is stored in separate JSON files, which are then stored in their own dataframes and are appended to the main dataframe using the `concat` function.

### Extract from AWS S3

Lastly, we need to extract data from an AWS S3 bucket. Below, you can see the csv file of the Products table, as well as the JSON file containing the date details corresponding to transactions. In order to access these files, we will use the boto3 library to interact with the S3 bucket. The bucket in question is defined, in this case, as `my_bucket`, and then the files are downloaded and converted into dataframes.

```python
def extract_from_s3(self, address = 's3://data-handling-public/products.csv'):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('products.csv', 'products.csv')
        df = pd.read_csv('products.csv')
        print(df.head())
        return df
```
```python
def extract_from_json(self, address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('date_details.json', 'date_details.json')
        df = pd.read_json('date_details.json')
        print(df.head())
        return df
```

