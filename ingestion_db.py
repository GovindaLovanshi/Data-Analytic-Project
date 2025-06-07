import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode = 'a'

)

engine =  create_engine('sqlite:///inventory.db')
def ingest_db(df,table_name,engine):
    '''this function ingests data into a database'''
    df.to_sql(table_name,con = engine,if_exists = 'replace',index =False)

def load_raw_data():
    '''this function loads the raw data from the csv file'''
    start = time.time()
    for files in os.listdir('data'):
       if 'csv' in files:
          df = pd.read_csv('data/'+ files)
          logging.info(f'Ingesting {files} in db')
          ingest_db(df,files[:-4],engine)
    end = time.time()
    total_time = (end-start)/60
    logging.info('Ingestion Complete')
    logging.info(f'\nTotal ingestion time: {total_time} minutes')


if __name__ == '__main__':
    load_raw_data() 

