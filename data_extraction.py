import pandas as pd
from database_utils import DatabaseConnector
import json
import requests
import tabula
import boto3
from urllib.request import urlopen

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
        # print(num_of_store.status_code)
        print(num_of_store)
        return num_of_store
    
    def retrieve_stores_data(self, headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}):
        num_of_store = self.list_number_of_stores()
        num_of_store = num_of_store['number_stores']
        df = pd.DataFrame()
        # store_data = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1', headers = headers)
        # print(store_data.status_code)
        # print(store_data.json())
        #store_list = []
        #store_number = 0
        for store_number in range(num_of_store):
            # store_number += 1
            store_data = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers = headers)
            x = store_data.json()
            # print(x)
            df2 = pd.json_normalize(x)
            df = pd.concat([df, df2])
        
        #print(df.head(10))
            # dfs = pd.concat([df, dfs])
        # print(df)
        return df
    
    def extract_from_s3(self, address = 's3://data-handling-public/products.csv'):
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('products.csv', 'products.csv')
        df = pd.read_csv('products.csv')
        print(df.head())
        return df
    
    def extract_from_json(self, address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        # json_file = requests.get(address)
        # df = json_file.json()
        # df = pd.json_normalize(json_file)
        # response = urlopen(address)
        # data_json = json.loads(response.read())
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('data-handling-public')
        my_bucket.download_file('date_details.json', 'date_details.json')
        df = pd.read_json('date_details.json')
        print(df.head())
        # df = pd.read_json(address)
        #print(df)
        return df


if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.read_rds_table()
    extractor.retrieve_pdf_data()
    extractor.list_number_of_stores()
    extractor.retrieve_stores_data()
    extractor.extract_from_s3()
    extractor.extract_from_json()


    
