# Multinational Retail Data Centralisation

> This project involves being part of a multinational organisation whose data are spread across a range of sources, with the objective being extracting the data, cleaning it using the Pandas library and uploading it to a PostgreSQL database for querying. 

## Summary

Following the ETL (Extract, Transform, Load) principle, data is collected from various sources, cleaned and sent to a database. This process will be performed using three separate scripts available within this repo: database_utils.py, data_extraction.py and data_cleaning.py. The former will connect to an AWS RDS database, the second will extract the data from various sources (RDS tables, API's, S3 bucket etc.), and the latter will clean the data and load it to a database. Once uploaded to the database, the date types for each column will be changed to the correct types in order for the database to be fit for querying. 

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
## Data Cleaning

> After extracting the data from various sources, we will now clean the data with the help of the Pandas library in the data_cleaning.py script.

In the data_cleaning.py file, the `DataCleaning` class is created, and within the __init__ method of this class, the `DataExtraction` and `DatabaseConnector` methods are imported.

In the subsequent methods of the `DataCleaning` class, the tables are cleaned with the Pandas library. Below is the `clean_products_data` method, which is used to clean the products table. We've used lambda functions numerous times to ensure that values in a given column meet the necessary criteria. We've also used `pd.to_datetime` to ensure that all values in the 'date_added' column are in the correct date format. For example, a value in that column may appear as a string i.e. '5 September 2005', therefore `pd.to_datetime` will convert the string to '2005/09/05', which is important for the dates to be able to be used in SQL. We've also performed other operations like removing £ signs from price values and dropping rows where values belonging to specific columns are NULL. This process is repeated for the other tables.

```python
def clean_products_data(self):
        product_data = self.convert_product_weights()
        valid_categories = ['homeware', 'toys-and-games', 'food-and-drink', 'pets', 'sports-and-leisure', 'health-and-beauty', 'diy']
        product_data['category'] = product_data['category'].apply(lambda x: x if x in valid_categories else np.nan)
        valid_availability = ['Still_avaliable', 'Removed']
        product_data['removed'] = product_data['removed'].apply(lambda x: x if x in valid_availability else np.nan).replace('Still_avaliable', 'Still_available')
        product_data['date_added'] = pd.to_datetime(product_data['date_added'], format='%Y-%m-%d', errors = 'coerce').dt.date
        product_data['product_price'] = pd.to_numeric(product_data['product_price'].str.strip('£'), errors = 'coerce').round(2)
        product_data['uuid'] = product_data['uuid'].apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
        product_data = product_data.rename(columns = {'EAN':'ean'})
        product_data['ean'] = product_data['ean'].apply(lambda x: str(x) if re.match('^[0-9]{12,13}$', str(x)) else np.nan)
        product_data['product_code'] = product_data['product_code'].apply(lambda x: x if re.match('^[a-zA-Z0-9]{2}-[0-9]{5,7}[a-zA-Z]$', str(x)) else np.nan)
        product_data.drop(['unit'], axis = 1, inplace = True)
        product_data = product_data.drop(columns = 'Unnamed: 0')
        product_data = product_data.dropna(subset = ['product_code'])
        product_data = product_data.drop_duplicates().reset_index(drop = True)
        product_data_table = self.connector.upload_to_db(product_data, 'dim_products')
        return product_data_table
```
The cleaned data is sent to the PostgreSQL database with the help of SQLAlchemy via the `upload_to_db` method, which is derived from the `DatabaseConnector` class imported from database_utils.py. The method takes 2 arguments: The dataframe in question and the name we want to give the table when uploaded to the database. We have to connect to the database using credentials, specifically a 'user', 'password', 'host', 'database'. The code corresponding to the `upload_to_db` method can be found below:

```python
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
## Creating the database schema

> Here, we will be casting all columns to the correct data types, so that when it comes to querying the tables to answe business questions, we will be able to compute the necessary aggregations, and the STAR-based schema can be created, using the columns in the orders table as the foreign keys.

### Casting the columns

Below, you can see a query that casts the columns of the products table to the correct data types, using `ALTER COLUMN`. It shows, for example, the product_price and weight columns being cast to floats in order for calculations to be performed correctly on them.

```sql
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::float,
ALTER COLUMN weight TYPE FLOAT USING weight::float,
ALTER COLUMN "ean" TYPE VARCHAR(20) USING "ean"::varchar(20),
ALTER COLUMN product_code TYPE VARCHAR(15) USING product_code::varchar(15),
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING still_available::bool,
ALTER COLUMN weight_class TYPE VARCHAR(20) USING weight_class::varchar(20)
```
We also add a new column called 'weight_class', using `ADD COLUMN`. The purpose of dding this column is to help the delivery team quickly make decisions on how to handle products based on their weight. We use the `CASE` function to assign human-readable values to the 'weight_class' column based on the values in the weight column.

```sql
ALTER TABLE dim_products
ADD weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = 
CASE WHEN weight < 3 THEN 'Light' 
WHEN weight BETWEEN 3 AND 40 THEN 'Mid_Sized' 
WHEN weight BETWEEN 41 and 140 THEN 'Heavy' 
ELSE 'Truck_Required' 
END;
```
### Adding the primary and foreign keys

Finally, in order to finalise the STAR-based schema, we will add the primary keys to all the tables exluding the orders table (This will be the main reference table), as well as adding thee foreign keys to the orders table.

## Querying the tables

### How many stores does the business have and in which countries?

This SQL query shows the number of stores in each country. GB has the most with 265, then Germany with 141 and then the USA with 34. We use `COUNT(country_code)` to compute the number of rows that have country codes, as well as `GROUP BY` to group the number of stores for each country.

```sql
SELECT country_code AS country, COUNT(country_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;
```

### Which locations currently have the most stores?

When running the following query, it can be concluded that Chapletown has the most stores with 14. The query follows similar principles to the previous query, however this time, we're using `ORDER BY` to order the rows from highest number of stores to lowest by also including `DESC`, and applying a limit of 7 so that only the first 7 locations with the highest number of stores are included.

```sql
SELECT locality, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC LIMIT 7;
```

### Which months produce the most sales typically?

Using the below query, the highest sales are produced in August while the lowest are produced in March. The query uses `LEFT JOIN` to connect the orders table to the date and products tables in such a way that the query will return all rows from the orders table and all the rows from the other 2 tables that match the condition. It uses the `SUM` aggregation to add up the multiplied values of the product quantity in the orders table and the product price in the products table, grouping the rows for each month using `GROUP BY`. We order the query by the sum of sales in descending order, applying a limit of 6 rows this time.

```sql
SELECT ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, dt.month
FROM orders_table AS o
LEFT JOIN dim_date_times AS dt
ON o.date_uuid = dt.date_uuid
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
GROUP BY dt.month
ORDER BY total_sales DESC LIMIT 6;
```
### How many sales are coming from online?

Running the query below shows that 26957 sales were made online, with 107739 products being sold. `CASE` is used to create a new column called 'location' to show if the store is Online or Offline based on their store type.

```sql
SELECT COUNT(*) AS number_of_sales, SUM(o.product_quantity) AS product_quantity_count,
CASE 
	WHEN store_type != 'Web Portal' THEN 'Offline'
	ELSE 'Web'
END AS location
FROM orders_table AS o
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
GROUP BY location;
```
### What percentage of sales come through each type of store?

The query below provides the percentages of sales for each store type, with the local store type generating the most. We use `LEFT JOIN` for the same purpose as in the third query. Aggregations are used to find the total sales, while we use the percentage formula to calculate the percentage. We also use `ROUND` to round the total sales and percentages to 2 decimal places.

```sql
SELECT s.store_type, ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, 
ROUND(COUNT(*) * 100/SUM(COUNT(*)) OVER ():: numeric, 2) AS percentage_total
FROM orders_table AS o
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
GROUP BY s.store_type
ORDER BY total_sales DESC;
```

### Which month in each year produced the most sales?

The query below shows the total sales for each month for each year, which can be used to find the month with the highest sales for a given year. For example, we can see that the company's highest ever sales figures were generated in March 1994. This query uses the total sales found using the orders table when connected to the products table, using `LEFT JOIN`, while also being joined to the dates table to get the year and month columns, which which are the columns we will be grouping the data by.
```sql
SELECT ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, dt.year, dt.month
FROM orders_table AS o
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
LEFT JOIN dim_date_times AS dt
ON o.date_uuid = dt.date_uuid
GROUP BY dt.year, dt.month
ORDER BY total_sales DESC LIMIT 10;
```
### What is our staff headcount?

The query below shows the staff headcount for each country, from which it can be deduced that Great Britain has the highest number of staff. It uses `SUM` to add up the staff numbers for each country. We use the `CASE` function to ensure we get the correct staff counts for each of the countries, grouping the data by this new column.

```sql
SELECT SUM(staff_numbers) AS total_staff_numbers,
CASE
	WHEN country_code = 'DE' THEN 'DE'
	WHEN country_code = 'US' THEN 'US'
	ELSE 'GB'
END AS country_code
FROM dim_store_details 
GROUP BY country_code
ORDER BY total_staff_numbers DESC;
```

### Which German store type is selling the most?

This query finds out which store type in Germany makes the most sales. It uses `SUM` to find the total sales using the orders table when joined to the products table, using `LEFT JOIN`. The orders table is also joined to the stores table so that we can group the data by the store type and country code, using `HAVING` to ensure that only German stores are selected. We also order the data by total sales in ascending order.

```sql
SELECT ROUND(SUM(p.product_price * o.product_quantity)::numeric, 2) AS total_sales, s.store_type, s.country_code
FROM orders_table AS o
LEFT JOIN dim_products AS p
ON o.product_code = p.product_code
LEFT JOIN dim_store_details AS s
ON o.store_code = s.store_code
GROUP BY store_type, country_code
HAVING country_code = 'DE'
ORDER BY total_sales ASC;
```
### How quickly is the company making sales?

The final query is used to find out how quickly the company is making sales. 

Firstly, we use `WITH` to create a CTE(Common Table Expression), which acts as a temporary table, and within this table we will be concatenating the year, month and day with the timestamp and convert the result to a timestamp, as well as selecting the 'year' column from the dates table, and we'll call the resulting table 'CTE'; we'll then create another CTE and call it 'CTE1', which will use the columns from the previous CTE, as well as also having another column that uses the `LEAD` function; this function will allow us to compare the value of the present row with that of the next row that we choose to access data from, which in this case will be the following row after the present one, and is shown by using the digit 1 within the `LEAD` function; we will use `OVER` to define the set of records over which the `LEAD` function will be applied, and in this case we'll use the function to order the data by the timestamp values in descending order, and we'll call this column 'time_difference'. 

Finally in the outer query, we'll select the 'year', as well as creating an aggregate function to find the average of the difference between the 'datetimes' and 'time_difference' columns and call it 'actual_time_taken'. We'll group the data by years so that the aggregate function is only applied to 'datetimes' values corresponding to the same year, as well as ordering the data by 'actual_time_taken' in descending order so that we can see the years in which sales are made more slowly at the top.

```sql
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS datetimes, year 
    FROM dim_date_times
    ORDER BY datetimes DESC
), 
cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) AS time_difference 
        FROM cte
) 
SELECT year, AVG((datetimes - time_difference)) AS actual_time_taken 
FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC;
```
