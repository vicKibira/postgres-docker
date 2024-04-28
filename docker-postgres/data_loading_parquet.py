import os, sys, argparse
import pandas as pd
from time import time
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import logging

def main(params):
    user = params.user
    password = params.password
    host = params.host
    db = params.db
    table_name = params.table_name
    url = params.url
    port = params.port

    # Getting the file name from the URL
    file_name = url.rsplit('/', 1)[-1].strip()
    logging.info(f"Downloading {file_name}...")

    # Download the file from the URL
    os.system(f"wget {url} -O {file_name}")
    logging.info('\n')

    # Create SQL engine
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    # Read the file based on whether it's CSV or Parquet
    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=100)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else:
        logging.error('Error: Only .csv or .parquet files are allowed')
        sys.exit()

    # Create the table
    df.head(n=0).to_sql(table_name, con=engine, if_exists='replace')

    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        logging.info(f"Inserting batch {count}")

        b_start = time()
        batch_df.to_sql(table_name, con=engine, if_exists='append')
        b_end = time()

        logging.info(f"Inserted! Time taken: {b_end - b_start:10.3f} seconds.\n")
    t_end = time()
    logging.info(f"Completed! Total time taken was {t_end - t_start:10.3f} for {count} batches.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Loading data into Postgres database from a Parquet file')

    parser.add_argument('--user', required=True, help='User for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database in Postgres')
    parser.add_argument('--table_name', required=True, help='Table in Postgres')
    parser.add_argument('--url', required=True, help='URL for the CSV data')

    args = parser.parse_args()

    main(args)
