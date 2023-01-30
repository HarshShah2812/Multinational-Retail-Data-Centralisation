from sqlalchemy import create_engine 
from sqlalchemy import inspect
import yaml

class DatabaseConnector:

    def __init__(self):
        self.read_db_creds()
        self.init_db_engine()
        


    def read_db_creds(self):
       with open ('db_creds.yaml', 'r') as creds:
        creds_loaded = yaml.safe_load(creds)
        print(creds_loaded)
        return creds_loaded

    def init_db_engine(self):
        creds_loaded = self.read_db_creds()
        engine = create_engine(f"{creds_loaded['DATABASE_TYPE']}+{creds_loaded['DBAPI']}://{creds_loaded['RDS_USER']}:{creds_loaded['RDS_PASSWORD']}@{creds_loaded['RDS_HOST']}:{creds_loaded['RDS_PORT']}/{creds_loaded['RDS_DATABASE']}")
        engine_connect = engine.connect()
        return engine_connect
    
    def list_db_tables(self, engine):
        self.init_db_engine()
        inspector = inspect(engine)
        list_tables = inspector.get_table_names()
        return list_tables

if __name__ == '__main__':
    database_connector = DatabaseConnector()
