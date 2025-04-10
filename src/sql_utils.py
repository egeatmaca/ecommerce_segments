from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import os
from dotenv import load_dotenv


def get_engine(protocol=None, user=None, password=None, host=None, port=None, db=None):
    load_dotenv()
    protocol = protocol if protocol else 'postgresql+psycopg2'
    user = user if user else os.environ.get('POSTGRES_USER')
    password = password if password else os.environ.get('POSTGRES_PASSWORD')
    host = host if host else 'localhost'
    port = port if port else 5432
    db = db if db else os.environ.get('POSTGRES_DB')
    
    db_url = f'{protocol}://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(db_url)

    return engine

def make_read_query_func(engine):
    def read_query(query, verbose=True):
        if verbose:
            print(query, '\n')
            
        with engine.connect() as conn:
            df = sqlio.read_sql_query(query, conn)
        
        return df
    
    return read_query