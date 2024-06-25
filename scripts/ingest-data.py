import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    csv_file = params.csv_file
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=1000000)

    df = next(df_iter)

    df.searchDate = pd.to_datetime(df.searchDate)
    df.flightDate = pd.to_datetime(df.flightDate)
    df.baseFare = df.baseFare.astype("float16")
    df.baseFare = df.baseFare.astype("float16")
    df.baseFare = df.baseFare.astype("float16")
    df.totalFare = df.totalFare.astype("float16")
    df.seatsRemaining = df.seatsRemaining.astype("int16")
    df.elapsedDays = df.elapsedDays.astype("int16")
    df.totalTravelDistance = df.totalTravelDistance.astype("float16")

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.searchDate = pd.to_datetime(df.searchDate)
            df.flightDate = pd.to_datetime(df.flightDate)
            df.baseFare = df.baseFare.astype("float16")
            df.baseFare = df.baseFare.astype("float16")
            df.baseFare = df.baseFare.astype("float16")
            df.totalFare = df.totalFare.astype("float16")
            df.seatsRemaining = df.seatsRemaining.astype("int16")
            df.elapsedDays = df.elapsedDays.astype("int16")
            df.totalTravelDistance = df.totalTravelDistance.astype("float16")


            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--csv_file', required=True, help='csv_file of the csv file')

    args = parser.parse_args()

    main(args)