import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format='%(asctime)s-%(levelname)s-%(message)s',
    filemode='a'
)


engine = create_engine('sqlite:///inventory.db')
def ingest_db(df, table_name, engine, chunksize=50000):
    '''this function will ingect the dataframe into database table'''
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)


def load_row_data():
    '''this function will load the csv as dataframe and innject into db'''
    start=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            for chunk in pd.read_csv(os.path.join('data', file), chunksize=100000):
                ingest_db(chunk, file[:-4], engine)
            logging.info(f'Ingesting {file} in db')
            
    end=time.time()
    total_time=(end-start)/60
    logging.info('-----Ingection Complete-----')
    logging.info(f'\n Total Time Taken: {total_time} minutes')
    
    
if __name__=='__main__':
    load_row_data()