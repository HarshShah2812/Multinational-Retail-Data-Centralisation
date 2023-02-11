import pandas as pd
from database_utils import DatabaseConnector
import tabula
class DataExtractor:
    
    def __init__(self):
        self.connector = DatabaseConnector()
       

    def read_rds_table(self, table_name):
        #connector = DatabaseConnector()
        #table_name = connector.list_db_tables()
        engine = self.connector.init_db_engine()
        df = pd.read_sql_table(table_name, engine, index_col = 'index')
        return df
    
    def retrieve_pdf_data(self, link):
        tab = tabula.read_pdf(link, pages = "all")
        df = pd.DataFrame(tab[0])
        return df



    
if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.read_rds_table()
    extractor.retrieve_pdf_data()


    
