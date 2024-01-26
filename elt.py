import os
import gzip
import time
import shutil
import requests
import logging
import argparse
import pandas as pd
from google.cloud import storage

from dotenv import load_dotenv
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

def extract_data(url: str, file_path: str, compress: bool = False):
    # Download dataset
    logging.info('Downloading from URL: %s ...', url)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception('Could not download file')
    logging.info('Download complete')
    
    # Write dataset to file
    logging.info('Writing to local file: %s ...', file_path)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    logging.info('Write complete')

    # If compress flag is set, compress data file
    if compress:
        logging.info('Compressing data file: %s ...', file_path)
        with open(file_path, 'rb') as f_in:
            with gzip.open(f'{file_path}.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        file_path += '.gz'
        logging.info('Compressed file path: %s', file_path)

    return pd.read_parquet(file_path)

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str):
    # Connect to GCS
    client = storage.Client()

    # Get bucket
    bucket = client.get_bucket(bucket_name)

    # Get blob
    blob = bucket.blob(blob_name)

    # Convert dataframe to csv string
    csv_str = df.to_csv(index=False)

    # Upload csv string to blob
    blob.upload_from_string(csv_str, content_type='text/csv')

    # Return blob uri
    return blob.public_url

def upload_to_postgres(df: pd.DataFrame, table_name: str, schema: str):
    # Read environment variables
    load_dotenv()

    # Load environment variables
    user    = os.getenv('POSTGRES_USER')
    pwd     = os.getenv('POSTGRES_PASSWORD')
    host    = os.getenv('POSTGRES_HOST')
    port    = os.getenv('POSTGRES_PORT')
    dbname  = os.getenv('POSTGRES_DB')
    schema  = os.getenv('POSTGRES_SCHEMA', schema)
    
    # Construct database uri
    uri = f'postgresql://{user}:{pwd}@{host}:{port}/{dbname}'
    
    # Create database engine
    engine = create_engine(uri)

    # Connect to database
    with engine.connect() as con:

        # Create schema if it doesn't exist
        logging.info('Creating schema: %s', schema)
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};').fetchall()

        # Upload dataset to database
        logging.info('Uploading to `ny_taxi` database')
        df.to_sql(name=table_name, con=con, schema=schema,
                  index=False, chunksize=100_000,
                  if_exists='append', method='multi')
        logging.info('Upload complete')

    # Dispose of engine connection
    engine.dispose()


def main(params):

    # Get pipeline parameters
    service = params.service
    year    = params.year
    month   = params.month
    schema  = params.schema

    # Construct data url, file & table name
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet'
    file_path = f'{service}_tripdata_{year}-{month:02d}.parquet'
    table_name = f'{service}_taxi_trips'

    # EXTRACT: data file #
    df = extract_data(url, file_path)

    logging.info('Dataframe shape: %s', df.shape)

    # LOAD: data file to GCS #
    pass

    # LOAD: data file to Postgres #
    upload_to_postgres(df, table_name, schema)



if __name__ == '__main__':
    print()
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('-t', '--taxi_service', required=True, dest='service',
                        help='taxi service type, options: "yellow", "green", "fhv".')
    parser.add_argument('-y', '--year', required=True, type=int,
                        help='year in which to query taxi trips')
    parser.add_argument('-m', '--month', required=True, type=int,
                        help='month of year to query taxi trip data')
    parser.add_argument('-s', '--schema', required=False,
                        help='schema to load data into', default='raw')
                        

    args = parser.parse_args()

    main(args)

    print()