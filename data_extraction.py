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
    
    def retrieve_pdf_data(self, link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        tab = tabula.read_pdf(link, pages = "all")
        df = pd.DataFrame(tab[0])
        print(df.head())
        return df
    
    def list_number_of_stores(self, headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        num_of_store = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers = headers)
        num_of_store = num_of_store.json()
        print(num_of_store)
        return num_of_store
    
    def retrieve_stores_data(self, headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        num_of_store = self.list_number_of_stores()
        num_of_store = num_of_store['number_stores']
        df = pd.DataFrame()
        for store_number in range(num_of_store):
            store_data = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers = headers)
            x = store_data.json()
            df2 = pd.json_normalize(x)
            df = pd.concat([df, df2])
        return df
    
    def extract_from_s3(self, address = 's3://data-handling-public/products.csv'):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('products.csv', 'products.csv')
        df = pd.read_csv('products.csv')
        print(df.head())
        return df
    
    def extract_from_json(self, address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('date_details.json', 'date_details.json')
        df = pd.read_json('date_details.json')
        print(df.head())
        return df

if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.read_rds_table()
    extractor.retrieve_pdf_data()
    extractor.list_number_of_stores()
    extractor.retrieve_stores_data()
    extractor.extract_from_s3()
    extractor.extract_from_json()


    
