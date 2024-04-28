import pandas as pd
import os
import argparse
from sqlalchemy import create_engine
import logging
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Download the data
    if url.endswith('.csv.gz'):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    # Correcting the wget command
    os.system(f"wget  --no-check-certificate {url} -O {csv_name}")

    # Create the database connection
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # A little bit of parsing
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create a table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
        
            # A little bit of parsing
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            logging.info('Inserted another chunk, it took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            logging.info("Finished loading data into the Postgres database")
            break

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Loading data into Postgres database')

    parser.add_argument('--user', required=True, help='User for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database in Postgres')
    parser.add_argument('--table_name', required=True, help='Table in Postgres')
    parser.add_argument('--url', required=True, help='URL for the CSV data')

    args = parser.parse_args()

    main(args)
