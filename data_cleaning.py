# %%
import pandas as pd
import numpy as np
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
    cleaner.clean_store_data()
# %%
