import pandas as pd
from sqlalchemy import create_engine 
from sqlalchemy import inspect
import yaml
import psycopg2


class DatabaseConnector:

    def __init__(self):
        #self.read_db_creds()
        #self.init_db_engine()
        self.list_db_tables()
        self.connection = psycopg2.connect(host = "localhost", database = "Sales_Data", user = "postgres", password = "Indiarule2_")

    def read_db_creds(self):
       with open ('db_creds.yaml', 'r') as creds:
        creds_loaded = yaml.safe_load(creds)
        #print(creds_loaded)
        return creds_loaded

    def init_db_engine(self):
        creds_loaded = self.read_db_creds()
        #engine = create_engine(f"{creds_loaded['Credentials']['DATABASE_TYPE']}+{creds_loaded[0]['DBAPI']}://{creds_loaded['RDS_USER']}:{creds_loaded['RDS_PASSWORD']}@{creds_loaded['RDS_HOST']}:{creds_loaded['RDS_PORT']}/{creds_loaded['RDS_DATABASE']}")
        engine = create_engine(f"{creds_loaded['DATABASE_TYPE']}+{creds_loaded['DBAPI']}://{creds_loaded['RDS_USER']}:{creds_loaded['RDS_PASSWORD']}@{creds_loaded['RDS_HOST']}:{creds_loaded['RDS_PORT']}/{creds_loaded['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        list_tables = inspector.get_table_names()
        #print(list_tables)
        return list_tables

    def upload_to_db(self, dataframe, table_name):
        conn_string = 'postgresql://postgres:Indiarule2_@localhost/Sales_Data'
        db = create_engine(conn_string)
        conn = db.connect()
        conn1 = self.connection #psycopg2.connect(database = "Sales_Data", user = "postgres", password = "Indiarule2_", host = "localhost")
        conn1.autocommit = True
        # cursor = conn1.cursor()
        # cursor.execute('drop table if exists table')
        df = pd.DataFrame(dataframe)
        df_sql = df.to_sql(table_name, conn, if_exists = 'replace')
        return df_sql


if __name__ == '__main__':
    database_connector = DatabaseConnector()

