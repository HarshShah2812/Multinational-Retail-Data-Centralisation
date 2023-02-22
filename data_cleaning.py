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
        # print(read_user_data.head())
        # print(read_user_data['date_of_birth'].describe())
        # print(read_user_data.duplicated(subset=['date_of_birth'], keep = False))
        # print(read_user_data['date_of_birth'].duplicated().sum())
        # print(read_user_data.sort_values(by=['date_of_birth'], ascending = False))
        read_user_data = read_user_data[~read_user_data['date_of_birth'].str.contains("[a-zA-Z]").fillna(False)]
        read_user_data.reset_index(drop = True, inplace = True)
        # read_user_data['date_of_birth'] = pd.to_datetime(read_user_data['date_of_birth'], format = '%Y-%m-%d', utc = False)
        # read_user_data['date_of_birth'] = pd.to_datetime(read_user_data['date_of_birth']).dt.date
        read_user_data['date_of_birth'] = pd.to_datetime(read_user_data['date_of_birth']).dt.date
        #read_user_data['date_of_birth'] = read_user_data['date_of_birth'].apply(pd.to_datetime).dt.date
        # print(read_user_data.sort_values(by=['date_of_birth'], ascending = False))
        # print(read_user_data.sort_values(by=['join_date'], ascending = False))
        # print(read_user_data[read_user_data['index']==202])
        # read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date'], format = '%M &Y %d', errors='ignore', utc = False).astype('datetime64[D]')
        # #read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date'], format = '%M &Y %d', errors='ignore', utc= False).astype('datetime64[D]')
        # read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date'], format = '%Y-%m-%d', errors='ignore', utc = False).astype('datetime64[D]')
        read_user_data['join_date'] = pd.to_datetime(read_user_data['join_date']).dt.date
        # read_user_data[['join_date', 'date_of_birth']] = pd.to_datetime(read_user_data[['join_date', 'date_of_birth']]).dt.date
        #read_user_data['join_date'] = read_user_data['join_date'].dt.date
        # print(read_user_data.sort_values(by=['join_date'], ascending = False))
        # print(read_user_data.isna().sum())
        # print(read_user_data.isnull().sum())
        read_user_data.info()

        print(set(read_user_data['country_code']))
        read_user_data['country_code'] = read_user_data['country_code'].replace({'GGB':'GB'})
        print(set(read_user_data['country_code']))
        # print(read_user_data['email_address'].value_counts())
        #print(set(read_user_data['phone_number']))
        # mapping = {"+44(0)":"00"}
        # read_user_data['phone_number'] = read_user_data['phone_number'].replace(mapping)
       
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('[^\d]+', '')
        read_user_data['phone_number'] = read_user_data['phone_number'].str.replace('44', '0')
        
        # print(set(read_user_data['phone_number']))
        
        # print(read_user_data['address'].iloc[0])
        read_user_data['address'] = read_user_data['address'].str.replace('\n', ' ')
        read_user_data['address'] = read_user_data['address'].str.upper()
        read_user_data['address'] = read_user_data['address'].str.replace('/', '')
        #read_user_data['address'] = read_user_data['address'].str.replace('-', ' ')
        
        # print(set(read_user_data['address']))
        print(read_user_data['address'])
        print(read_user_data)

        user_table = self.connector.upload_to_db(read_user_data, 'dim_users')
        return user_table
        
    def clean_card_data(self):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        read_card_data = self.extractor.retrieve_pdf_data(link)
        read_card_data.info()
        # print(read_card_data.isnull().sum())
        # print(read_card_data.isna().sum())
        # print(read_card_data)
        read_card_data['expiry_date'] = pd.to_datetime(read_card_data['expiry_date'], format = '%m/%y', errors = 'ignore', infer_datetime_format = True)
        read_card_data['expiry_date'] = read_card_data['expiry_date'] + MonthEnd(0)
        read_card_data['expiry_date'] = read_card_data['expiry_date'].dt.date
        read_card_data['date_payment_confirmed'] = pd.to_datetime(read_card_data['date_payment_confirmed']).dt.date
        # read_card_data.info()
        card_details_table = self.connector.upload_to_db(read_card_data, 'dim_card_details')
        return card_details_table

    def clean_store_data(self):
        store_data = self.extractor.retrieve_stores_data()
        store_data.info()
        store_data.set_index('index', drop = True, inplace = True)
        store_data['address'] = store_data['address'].str.replace('\n', ' ')
        store_data['address'] = store_data['address'].str.replace(',', '')
        store_data['address'] = store_data['address'].str.upper()
        # print(store_data.head(15))
        # #print(store_data['address'])
        # print(store_data['address'].describe())
        # print(store_data[store_data['address'].duplicated()])
        # print(store_data.iloc[[405]])
        # print(store_data.iloc[[437]])
        store_data.drop([405], inplace = True)
        store_data.drop([437], inplace = True)
        # print(store_data['address'].describe())
        # print(store_data[store_data['address'].duplicated()])
        # print(store_data.sort_values(by=['lat'], ascending = True))
        store_data = store_data[~store_data['latitude'].str.contains("[a-zA-Z]").fillna(False)]
        # print(store_data.sort_values(by=['lat'], ascending = True))
        store_data['longitude'] = store_data['longitude'].replace({'N/A': None})
        store_data['longitude'] = store_data['longitude'].astype(float)
        # print(store_data.iloc[[447]])
        store_data.reset_index(drop = True, inplace = True)
        # print(store_data.tail(15))
        # print(set(store_data['lat']))
        store_data.drop(columns = ['lat'], inplace = True)
        # print(store_data.head(15))
        # print(store_data.describe())
        # print(set(store_data['staff_numbers']))
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace('[a-zA-Z]', '')
        store_data['staff_numbers'] = store_data['staff_numbers'].astype(int)
        # print(set(store_data['staff_numbers']))
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date']).dt.date
        store_data['latitude'] = store_data['latitude'].astype(float)
        #store_data['latitude'] = store_data['latitude'].apply(lambda x: np.round(x, 2))
        # print(set(store_data['latitude']))
        # print(set(store_data['opening_date']))
        # print(set(store_data['store_type']))
        # print(set(store_data['country_code']))
        # print(set(store_data['continent']))
        store_data['country_code'] = store_data['country_code'].replace({None: 'N/A'})
        store_data['continent'] = store_data['continent'].replace({'eeEurope': 'Europe'})
        store_data['continent'] = store_data['continent'].replace({'eeAmerica': 'America'})
        store_data['continent'] = store_data['continent'].replace({None: 'N/A'})
        # print(set(store_data['continent']))
        store_data_table = self.connector.upload_to_db(store_data, 'dim_store_details')
        return store_data_table
       
    def convert_product_weights(self):
        product_data = self.extractor.extract_from_s3()
        # print(set(product_data['weight']))
        
        # print(product_data.iloc[[1779]])
        # print(product_data.loc[product_data['weight'] == 'Z8ZTDGUZVU'])
        product_data['unit'] =  product_data['weight'].str.replace('[0-9.,/-]+', '', regex = True)
        product_data['weight'] =  product_data['weight'].str.replace('x', '-')
        # print(set(product_data['weight']))
        # print(product_data.loc[product_data['weight'] == '5 * 145g'])
        product_data['weight'] =  product_data['weight'].str.replace('\s', '', regex = True)
        product_data['weight'] =  product_data['weight'].str.replace(r'[a-zA-Z$]+', '', regex = True)
        
        df1 = pd.DataFrame(product_data['weight'])
        # print(df1)
        df1['weight1'] = df1['weight'].str.split("-").str[0].astype(float).round(2)
        df1['weight2'] = df1['weight'].str.split("-").str[1].astype(float).round(2)
        # print(df1)
        # print(df1.iloc[[1690]])
        # print(df1.iloc[[1701]])
        # print(set(df1['weight2']))
        df1['weight2'] = df1['weight2'].replace({np.nan : float(0)})
        df1['weight1'] = df1['weight2'].apply(lambda x: x if x > 0 else 1) * df1['weight1']
        product_data['weight'] = df1['weight1'].round(2)
        # product_data['weight'] =  product_data['weight'].str.replace('[\d * \d]', float([\d * ).astype(float).round(2)
        # print(product_data.iloc[[1690]])
        # print(product_data.iloc[[1701]])
        # #product_data['weight'] =  product_data['weight'].str.replace(r'\b\d+\s+', '', regex = True).astype(float).round(2)
        # print(product_data.iloc[[1779]])
        # print(product_data.iloc[[1133]])
        # product_data['weight'] =  product_data['weight'].str.replace(r'\b.', '', regex = True)
        #product_data['weight'] =  product_data['weight'].str.replace('[\b.]', '', regex = True) #.astype(float).round(3)
        #product_data['weight'] =  product_data['weight'].str.extract('(\d+)').astype(float)
        # print(set(product_data['unit']))
        # print(set(product_data['weight']))
        # print(product_data.sort_values(by=['weight'], ascending = False))
        product_data['unit'] = product_data['unit'].replace({' x g': 'g'})
        product_data['unit'] = product_data['unit'].replace({'g ': 'g'})
        # print(set(product_data['unit']))
        # print(product_data.iloc[[1690]])
        # print(product_data.iloc[[1701]])
        # print(set(product_data['weight']))
        
        product_data['weight'] = np.where(product_data['unit'] == 'g', product_data['weight']/1000, product_data['weight']).round(3)
        product_data['weight'] = np.where(product_data['unit'] == 'ml', product_data['weight']/1000, product_data['weight']).round(3)
        product_data['weight'] = np.where(product_data['unit'] == 'oz', product_data['weight']/35.274, product_data['weight']).round(3)
        # print(set(product_data['weight']))
        # print(product_data.iloc[[382]])
        # print(product_data.loc[product_data['unit'] == 'g'])
        # print(product_data.loc[product_data['unit'] == 'ml'])
        # print(product_data.loc[product_data['unit'] == 'oz'])
        # print(product_data.loc[product_data['weight'] == 35.5])

        return product_data
    
    def clean_products_data(self):

        product_data = self.convert_product_weights()
        print(product_data.info())
        product_data.rename({'Unnamed: 0': 'index'}, axis = 'columns', inplace = True)
        product_data.set_index('index', drop = True, inplace = True)
        product_data.rename({'weight': 'weight(kg)'}, axis = 'columns', inplace = True)
        print(product_data['weight(kg)'])
        print(product_data.info())
        print(set(product_data['product_price']))
        product_data['product_price'] = product_data['product_price'].str.replace('£', '')
        print(set(product_data['product_price']))
        product_data.rename({'product_price': 'product_price(£)'}, axis = 'columns', inplace = True)
        print(product_data.sort_values(by = ['product_price(£)'], ascending = False))
        product_data = product_data[~product_data['product_price(£)'].str.contains("[a-zA-Z]").fillna(False)]
        product_data = product_data.dropna(axis = 0)
        product_data.reset_index(drop = True, inplace = True)
        product_data['product_price(£)'] = product_data['product_price(£)'].astype(np.float64).round(2)
        print(set(product_data['product_price(£)']))
        print(product_data.sort_values(by = ['product_price(£)'], ascending = False))
        print(product_data.info())
        product_data['category'] = product_data['category'].str.replace('-', ' ')
        print(set(product_data['EAN']))
        product_data['EAN'] = product_data['EAN'].astype(np.int64)
        print(set(product_data['EAN']))
        print(product_data.info())
        print(set(product_data['date_added']))
        product_data['date_added'] = pd.to_datetime(product_data['date_added']).dt.date
        print(set(product_data['date_added']))
        print(product_data.info())
        print(set(product_data['removed']))
        product_data['removed'] = product_data['removed'].str.replace('_', ' ')
        print(set(product_data['product_code']))
        product_data['product_code'] = product_data['product_code'].str.upper()
        print(set(product_data['product_code']))
        print(product_data.head(20))
        print(set(product_data['category']))
        product_data.drop(['unit'], axis = 1, inplace = True)
        print(product_data.info())
        print(product_data)
        product_data_table = self.connector.upload_to_db(product_data, 'dim_products')
        return product_data_table
        # print(product_data['weight'].tail(50))

    def clean_orders_data(self):
        orders_data = self.extractor.read_rds_table("orders_table")
        orders_data.info()
        print(orders_data.head(15))
        orders_data.set_index('level_0', drop = True, inplace = True)
        orders_data.drop(['first_name', 'last_name', '1'], axis = 1, inplace = True)
        orders_data.reset_index(drop = True, inplace = True)
        print(orders_data.head(15))
        orders_data['product_code'] = orders_data['product_code'].str.upper()
        print(set(orders_data['product_code']))
        orders_data_table = self.connector.upload_to_db(orders_data, 'orders_table')
        return orders_data_table
    
    def clean_time_data(self):
        time_data = self.extractor.extract_from_json()
        time_data.info()
        # sales_data['year'] = sales_data['year'].astype(np.int64)
        
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
        time_data['month'] = time_data['month'].astype(np.int64)
        time_data['year'] = time_data['year'].astype(np.int64)
        time_data['day'] = time_data['day'].astype(np.int64)
        print(set(time_data['time_period']))
        time_data['time_period'] = time_data['time_period'].str.replace('_', ' ')
        print(set(time_data['time_period']))
        print(time_data.head(20))
        time_data.info()
        time_data_table = self.connector.upload_to_db(time_data, 'dim_date_times')
        return time_data_table
        # print(set(sales_data['timestamp']))
        # print(set(sales_data['date_uuid']))
        
        
        
        



        


        #print(read_user_data['email_address'])
        # %%
        # print(read_user_data.iloc[15183])
        # print(read_user_data.dtypes)
        # print(read_user_data.head())
        #print(set(read_user_data['date_of_birth']))


       




        
        
        
        
        # read_user_data['first_name'] = read_user_data['first_name'].apply(lambda x: str(self.spell(x)))
        # read_user_data['last_name'] = read_user_data['last_name'].apply(lambda x: str(self.spell(x)))
        # read_user_data['company'] = read_user_data['company'].apply(lambda x: str(self.spell(x)))
        # read_user_data['email_address'] = read_user_data['email_address'].apply(lambda x: str(self.spell(x)))
        # read_user_data['address'] = read_user_data['address'].apply(lambda x: str(self.spell(x)))
        # read_user_data['country'] = read_user_data['country'].apply(lambda x: str(self.spell(x)))

        

        #print(read_user_data)
        # print(read_user_data.sort_values(by=['join_date'], ascending = False))

        #read_user_data = read_user_data.drop([10373, 10224, 5309, 12197, 8398], inplace = True)
        
        #print(read_user_data.sort_values(by=['date_of_birth'], ascending = False))
        

if __name__ == "__main__":
    cleaner = DataCleaning()
    # cleaner.clean_user_data()
    # cleaner.clean_card_data()
    #cleaner.clean_store_data()
    #cleaner.convert_product_weights()
    #cleaner.clean_products_data()
    # cleaner.clean_orders_data()
    cleaner.clean_time_data()
# %%
