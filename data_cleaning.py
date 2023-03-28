# %%
import pandas as pd
import numpy as np
from datetime import datetime
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from pandas.tseries.offsets import MonthEnd
import re


class DataCleaning:
    def __init__(self):
        self.extractor = DataExtractor()
        self.connector = DatabaseConnector()
        
    
    def clean_user_data(self):
        read_user_data = self.extractor.read_rds_table("legacy_users")
        read_user_data.info()
        # read_user_data = read_user_data[~read_user_data['date_of_birth'].str.contains("[a-zA-Z]").fillna(False)]
        print(read_user_data.duplicated(subset = 'email_address', keep = False).tail(50))
        read_user_data.drop_duplicates(subset = 'email_address', keep = 'first', inplace = True)
        print(set(read_user_data['date_of_birth']))
        read_user_data['date_of_birth'] = pd.to_datetime(read_user_data['date_of_birth'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        print(set(read_user_data['join_date']))
        read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        read_user_data.info()

        print(set(read_user_data['country_code']))
        read_user_data['country_code'] = read_user_data['country_code'].replace({'GGB':'GB'})
        print(set(read_user_data['country_code']))
       
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('[^\d]+', '')
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('44', '0')
        read_user_data['address'] = read_user_data['address'].str.replace('\n', ' ')
        read_user_data['address'] = read_user_data['address'].str.upper()
        read_user_data['address'] = read_user_data['address'].str.replace('/', '')
        read_user_data.dropna(inplace = True, subset = ['date_of_birth', 'join_date'])
        read_user_data.reset_index(drop = True, inplace = True)
        print(read_user_data['address'])
        print(read_user_data)
        read_user_data.info()
        # user_table = self.connector.upload_to_db(read_user_data, 'dim_users')
        # return user_table
        
    def clean_card_data(self):
        read_card_data = self.extractor.retrieve_pdf_data()
        read_card_data.info()
        read_card_data['date_payment_confirmed'] = pd.to_datetime(read_card_data['date_payment_confirmed']).dt.date
        # card_details_table = self.connector.upload_to_db(read_card_data, 'dim_card_details')
        # return card_details_table

    def clean_store_data(self):
        store_data = self.extractor.retrieve_stores_data()
        store_data.info()
        store_data.set_index('index', drop = True, inplace = True)
        print(set(store_data['address']))
        # store_data['address'] = store_data['address'].apply(lambda x: np.nan if len(str(x).split()) == 1 else x)
        store_data['address'] = store_data['address'].str.replace('\n', ' ')
        store_data['address'] = store_data['address'].str.replace(',', '')
        store_data['address'] = store_data['address'].str.upper()
        # store_data['address'] = store_data['address'].replace({'N/A': None})
        store_data['address'] = store_data['address'].apply(lambda x: np.nan if len(str(x).split()) == 1 else x)
        # print(store_data.iloc[405])
        # print(store_data.iloc[437])
        # store_data.dropna(axis = 'rows', thresh = len(store_data.columns) - 1, inplace = True)
        # store_data.drop([405], inplace = True)
        # store_data.drop([437], inplace = True)
        print(set(store_data['address']))
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        store_data.info()
        continents = ['Europe', 'America']
        store_data['continent'] = store_data['continent'].apply(lambda x: x if x in continents else ('Europe' if 'Europe' in str(x) else ('America' if 'America' in str(x) else np.nan)))
        print(set(store_data['country_code']))
        country_codes = ['GB', 'US', 'DE']
        store_data['country_code'] = store_data['country_code'].apply(lambda x: x if x in country_codes else np.nan)
        print(set(store_data['country_code']))
        store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        store_data['store_type'] = store_data['store_type'].apply(lambda x: x if x in store_types else np.nan)
        store_data['locality'] = store_data['locality'].str.replace('\d', '', regex=True)
        print(set(store_data['store_code']))
        store_data['store_code'] = store_data['store_code'].apply(lambda x: x if re.match('^[A-Z]{2,3}-[A-Z0-9]{8}$', str(x)) else np.nan)
        # store_data = store_data[~store_data['latitude'].str.contains("[a-zA-Z]").fillna(False)]
        
        # store_data['longitude'] = store_data['longitude'].replace({'N/A': None})
        # store_data['longitude'] = store_data['longitude'].astype(float).round(1)
        print(set(store_data['locality']))
        # store_data['locality'] = store_data['locality'].replace({'N/A': None})
        # store_data.drop(columns = ['lat'], inplace = True)
        # store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('[a-zA-Z]', '')
        print(set(store_data['staff_numbers']))
        print(set(store_data['store_type']))
        print(set(store_data['opening_date']))
        # store_data['latitude'] = store_data['latitude'].astype(float).round(1)
        print(set(store_data['continent']))
        # store_data['continent'] = store_data['continent'].replace({'eeEurope': 'Europe'})
        # store_data['continent'] = store_data['continent'].replace({'eeAmerica': 'America'})
        store_data.at[0, 'country_code'] = None
        store_data.at[0, 'continent'] = None
        print(set(store_data['country_code']))
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('[a-zA-Z]', '')
        store_data[['staff_numbers', 'longitude', 'latitude']] = store_data[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: pd.to_numeric(x, errors='coerce').round(1))
        store_data.info()
        store_data.dropna(subset=['staff_numbers'], inplace=True)
        store_data['staff_numbers'] = store_data['staff_numbers'].astype(int)
        print(set(store_data['staff_numbers']))
        print(store_data[store_data['staff_numbers'].isna()])
        # store_data = store_data.astype({'staff_numbers': 'int64'})
        # store_data.drop(columns = ['index'], inplace = True)
        store_data.drop(['lat'], axis=1, inplace=True)
        store_data.info()
        store_data.dropna(inplace=True, subset=['store_code', 'store_type'])
        store_data.drop_duplicates(inplace = True)
        store_data.reset_index(drop = True, inplace = True)
        print(set(store_data['store_code']))
        store_data.info()
        print(store_data.head(20))
        print(store_data['opening_date'].head(20))
        # store_data_table = self.connector.upload_to_db(store_data, 'dim_store_details')
        # return store_data_table
       
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
        # product_data.rename({'Unnamed: 0': 'index'}, axis = 'columns', inplace = True)
        # product_data.set_index('index', drop = True, inplace = True)
        print(product_data['weight'])
        print(product_data.info())
        print(product_data.sort_values(by = ['product_price'], ascending = False))
        # product_data = product_data.dropna(axis = 0)
        valid_categories = ['homeware', 'toys-and-games', 'food-and-drink', 'pets', 'sports-and-leisure', 'health-and-beauty', 'diy']
        product_data['category'] = product_data['category'].apply(lambda x: x if x in valid_categories else np.nan)
        
        valid_availability = ['Still_avaliable', 'Removed']
        product_data['removed'] = product_data['removed'].apply(lambda x: x if x in valid_availability else np.nan).replace('Still_avaliable', 'Still_available')
        
        product_data['date_added'] = pd.to_datetime(product_data['date_added'], format='%Y-%m-%d', errors = 'coerce').dt.date
        
        product_data['product_price'] = pd.to_numeric(product_data['product_price'].str.strip('£'), errors = 'coerce').round(2)
        
        product_data['uuid'] = product_data['uuid'].apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
        
        product_data = product_data.rename(columns = {'EAN':'ean'})
        product_data['ean'] = product_data['ean'].apply(lambda x: str(x) if re.match('^[0-9]{12,13}$', str(x)) else np.nan)
        
        product_data['product_code'] = product_data['product_code'].apply(lambda x: x if re.match('^[a-zA-Z0-9]{2}-[0-9]{6,7}[a-zA-Z]$', str(x)) else np.nan)
        print(set(product_data['product_price']))
        print(product_data.sort_values(by = ['product_price'], ascending = False))
        print(product_data.info())
        print(set(product_data['category']))
        
        # product_data['category'] = product_data['category'].str.replace('-', ' ')
        # product_data['removed'] = product_data['removed'].str.replace('_', ' ')
        print(product_data.head(20))
        # product_data['removed'] = product_data['removed'].replace({'Still_avaliable': 'Still_available'})
        product_data.drop(['unit'], axis = 1, inplace = True)
        product_data = product_data.drop(columns = 'Unnamed: 0')
        product_data = product_data.dropna(subset = ['product_code'])
        product_data = product_data.drop_duplicates().reset_index(drop = True)
        print(product_data.info())
        print(product_data)
        print(set(product_data['product_code']))
        # product_data_table = self.connector.upload_to_db(product_data, 'dim_products')
        # return product_data_table

    def clean_orders_data(self):
        orders_data = self.extractor.read_rds_table("orders_table")
        orders_data.info()
        print(orders_data.head(15))
        orders_data.set_index('level_0', drop = True, inplace = True)
        orders_data.drop(['first_name', 'last_name', '1'], axis = 1, inplace = True)
        print(orders_data.head(15))
        print(set(orders_data['product_code']))
        orders_data.info()
        # orders_data_table = self.connector.upload_to_db(orders_data, 'orders_table')
        # return orders_data_table
    
    def clean_time_data(self):
        time_data = self.extractor.extract_from_json()
        time_data.info()
        # print(set(time_data['timestamp']))
        time_data.replace('NULL', np.nan, inplace = True)
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        # time_data = time_data[~time_data['month'].str.contains('[a-zA-Z]').fillna(False)]
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        print(time_data.head(20))
        time_data['timestamp'] = pd.to_datetime(time_data['timestamp'], format = '%H:%M:%S', errors = 'coerce').dt.time
        months = [*range(1,13)]
        time_data['month'] = pd.to_numeric(time_data['month'], errors='coerce')
        time_data['month'] = time_data['month'].apply(lambda x: x if x in months else np.nan)
        days = [*range(1,32)]
        time_data['day'] = pd.to_numeric(time_data['day'], errors='coerce')
        time_data['day'] = time_data['day'].apply(lambda x: x if x in days else np.nan)
        years = [*range(1980,2023)]
        time_data['year'] = pd.to_numeric(time_data['year'], errors='coerce')
        time_data['year'] = time_data['year'].apply(lambda x: x if x in years else np.nan)
        time_data['date_uuid'] = time_data['date_uuid'].apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)
        periods = ['Morning', 'Midday', 'Evening', 'Late_Hours']
        time_data['time_period'] = time_data['time_period'].apply(lambda x: x if x in periods else np.nan)
        print(set(time_data['date_uuid']))
        print(set(time_data['time_period']))
        # time_data['time_period'] = time_data['time_period'].str.replace('_', ' ')
        print(set(time_data['time_period']))
        print(time_data.head(20))
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        time_data.info()
        time_data.dropna(inplace=True, subset=['date_uuid'])
        time_data.drop_duplicates(inplace=True)
        time_data.reset_index(drop=True, inplace=True)
        time_data.info()
        print(set(time_data['month']))
        print(set(time_data['year']))
        print(set(time_data['day']))
        print(set(time_data['time_period']))
        # print(set(time_data['date_uuid']))
        # time_data_table = self.connector.upload_to_db(time_data, 'dim_date_times')
        # return time_data_table
        
if __name__ == "__main__":
    cleaner = DataCleaning()
    # cleaner.clean_user_data()
    # cleaner.clean_card_data()
    # cleaner.clean_store_data()
    cleaner.convert_product_weights()
    cleaner.clean_products_data()
    # cleaner.clean_orders_data()
    # cleaner.clean_time_data()
# %%
