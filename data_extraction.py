import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    
    def __init__(self):
        connector = DatabaseConnector()

    def read_rds_table(self, table_name):
        connector = DatabaseConnector()
        table_name = connector.list_db_tables()
        df = pd.read_sql_table(table_name)
        return df

    
