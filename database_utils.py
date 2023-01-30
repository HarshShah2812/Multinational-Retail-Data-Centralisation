from sqlalchemy import create_engine 
import yaml

class DatabaseConnector:

    def __init__(self):
        self.creds = {}


    def read_db_creds(self):
       with open ('db_creds.yaml', 'r') as creds:
        creds_loaded = yaml.safe_load(creds)
        print(creds_loaded)

    def init_db_engine(self):
        engine = create_engine()
