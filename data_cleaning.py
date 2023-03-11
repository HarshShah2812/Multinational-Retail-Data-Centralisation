# %%
import pandas as pd
import numpy as np
from datetime import datetime
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from pandas.tseries.offsets import MonthEnd


class DataCleaning:
    def __init__(self):
        self.extractor = DataExtractor()
        self.connector = DatabaseConnector()
        
    
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table("legacy_users")
        read_user_data.info()
        read_user_data = read_user_data[~read_user_data['date_of_birth'].str.contains("[a-zA-Z]").fillna(False)]
        read_user_data.reset_index(drop = True, inplace = True)
        read_user_data['date_of_birth'] = pd.to_datetime(read_user_data['date_of_birth']).dt.date
        read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date']).dt.date
        read_user_data.info()

        print(set(read_user_data['country_code']))
        read_user_data['country_code'] = read_user_data['country_code'].replace({'GGB':'GB'})
        print(set(read_user_data['country_code']))
       
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('[^\d]+', '')
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('44', '0')
        read_user_data['address'] = read_user_data['address'].str.replace('\n', ' ')
        read_user_data['address'] = read_user_data['address'].str.upper()
        read_user_data['address'] = read_user_data['address'].str.replace('/', '')
        print(read_user_data['address'])
        print(read_user_data)

        user_table = self.connector.upload_to_db(read_user_data, 'dim_users')
        return user_table
        
    def clean_card_data(self):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        read_card_data = self.extractor.retrieve_pdf_data(link)
        read_card_data.info()
        read_card_data['date_payment_confirmed'] = pd.to_datetime(read_card_data['date_payment_confirmed']).dt.date
        card_details_table = self.connector.upload_to_db(read_card_data, 'dim_card_details')
        return card_details_table

    def clean_store_data(self):
        store_data = self.extractor.retrieve_stores_data()
        store_data.info()
        store_data.set_index('index', drop = True, inplace = True)
        store_data['address'] = store_data['address'].str.replace('\n', ' ')
        store_data['address'] = store_data['address'].str.replace(',', '')
        store_data['address'] = store_data['address'].str.upper()
        store_data['address'] = store_data['address'].replace({'N/A': None})
        store_data.drop([405], inplace = True)
        store_data.drop([437], inplace = True)
       
        store_data = store_data[~store_data['latitude'].str.contains("[a-zA-Z]").fillna(False)]
        
        store_data['longitude'] = store_data['longitude'].replace({'N/A': None})
        store_data['longitude'] = store_data['longitude'].astype(float)
        store_data['locality'] = store_data['locality'].replace({'N/A': None})
        store_data.reset_index(drop = True, inplace = True)
        
        store_data.drop(columns = ['lat'], inplace = True)
       
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('[a-zA-Z]', '')
        store_data['staff_numbers'] = store_data['staff_numbers'].astype(int)
       
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date']).dt.date
        store_data['latitude'] = store_data['latitude'].astype(float)
        store_data['continent'] = store_data['continent'].replace({'eeEurope': 'Europe'})
        store_data['continent'] = store_data['continent'].replace({'eeAmerica': 'America'})
        store_data.at[0, 'country_code'] = None
        store_data.at[0, 'continent'] = None
        store_data_table = self.connector.upload_to_db(store_data, 'dim_store_details')
        return store_data_table
       
    def convert_product_weights(self):
        product_data = self.extractor.extract_from_s3()
        product_data['unit'] =  product_data['weight'].str.replace('[0-9.,/-]+', '', regex = True)
        product_data['weight'] =  product_data['weight'].str.replace('x', '-')
        product_data['weight'] =  product_data['weight'].str.replace('\s', '', regex = True)
        product_data['weight'] =  product_data['weight'].str.replace(r'[a-zA-Z$]+', '', regex = True)
        
        df1 = pd.DataFrame(product_data['weight'])
        df1['weight1'] = df1['weight'].str.split("-").str[0].astype(float).round(2)
        df1['weight2'] = df1['weight'].str.split("-").str[1].astype(float).round(2)
        df1['weight2'] = df1['weight2'].replace({np.nan : float(0)})
        df1['weight1'] = df1['weight2'].apply(lambda x: x if x > 0 else 1) * df1['weight1']
        product_data['weight'] = df1['weight1'].round(2)
        product_data['unit'] = product_data['unit'].replace({' x g': 'g'})
        product_data['unit'] = product_data['unit'].replace({'g ': 'g'})
        product_data['weight'] = np.where(product_data['unit'] == 'g', product_data['weight']/1000, product_data['weight']).round(3)
        product_data['weight'] = np.where(product_data['unit'] == 'ml', product_data['weight']/1000, product_data['weight']).round(3)
        product_data['weight'] = np.where(product_data['unit'] == 'oz', product_data['weight']/35.274, product_data['weight']).round(3)
        
        return product_data
    
    def clean_products_data(self):

        product_data = self.convert_product_weights()
        print(product_data.info())
        product_data.rename({'Unnamed: 0': 'index'}, axis = 'columns', inplace = True)
        product_data.set_index('index', drop = True, inplace = True)
        print(product_data['weight'])
        print(product_data.info())
        print(set(product_data['product_price']))
        product_data['product_price'] = product_data['product_price'].str.replace('£', '')
        print(set(product_data['product_price']))
        print(product_data.sort_values(by = ['product_price'], ascending = False))
        product_data = product_data[~product_data['product_price'].str.contains("[a-zA-Z]").fillna(False)]
        product_data = product_data.dropna(axis = 0)
        product_data.reset_index(drop = True, inplace = True)
        print(set(product_data['product_price']))
        print(product_data.sort_values(by = ['product_price'], ascending = False))
        print(product_data.info())
        product_data['category'] = product_data['category'].str.replace('-', ' ')
        print(product_data.info())
        product_data['date_added'] = pd.to_datetime(product_data['date_added']).dt.date
        print(product_data.info())
        product_data['removed'] = product_data['removed'].str.replace('_', ' ')
        print(product_data.head(20))
        product_data['removed'] = product_data['removed'].replace({'Still avaliable': 'Still available'})
        product_data.drop(['unit'], axis = 1, inplace = True)
        print(product_data.info())
        print(product_data)
        print(set(product_data['product_code']))
        product_data_table = self.connector.upload_to_db(product_data, 'dim_products')
        return product_data_table

    def clean_orders_data(self):
        orders_data = self.extractor.read_rds_table("orders_table")
        orders_data.info()
        print(orders_data.head(15))
        orders_data.set_index('level_0', drop = True, inplace = True)
        orders_data.drop(['first_name', 'last_name', '1'], axis = 1, inplace = True)
        orders_data.reset_index(drop = True, inplace = True)
        print(orders_data.head(15))
        print(set(orders_data['product_code']))
        orders_data_table = self.connector.upload_to_db(orders_data, 'orders_table')
        return orders_data_table
    
    def clean_time_data(self):
        time_data = self.extractor.extract_from_json()
        time_data.info()
        print(set(time_data['timestamp']))
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        time_data = time_data[~time_data['month'].str.contains('[a-zA-Z]').fillna(False)]
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        print(time_data.head(20))
        time_data['timestamp'] = pd.to_datetime(time_data['timestamp']).dt.time
        print(set(time_data['time_period']))
        time_data['time_period'] = time_data['time_period'].str.replace('_', ' ')
        print(set(time_data['time_period']))
        print(time_data.head(20))
        time_data.info()
        print(set(time_data['date_uuid']))
        time_data_table = self.connector.upload_to_db(time_data, 'dim_date_times')
        return time_data_table
        
if __name__ == "__main__":
    cleaner = DataCleaning()
    cleaner.clean_user_data()
    cleaner.clean_card_data()
    cleaner.clean_store_data()
    cleaner.convert_product_weights()
    cleaner.clean_products_data()
    cleaner.clean_orders_data()
    cleaner.clean_time_data()
# %%
