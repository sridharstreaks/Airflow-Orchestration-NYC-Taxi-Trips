import os
import urllib.request
import datetime
import requests
import duckdb
import glob
import pandas as pd

import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

#Postgres Details
user = "airflow"
password = "airflow"
host = "postgres"
port = 5432
db_name = "trips_data"

def check_db_conn():
        engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
        if not database_exists(engine.url):
            create_database(engine.url)
        elif database_exists(engine.url):
             print(f"{db_name} Exists")

# Define the start and end years
today = datetime.date.today()
end_year = today.year
start_year = today.year
taxis = ['yellow','green','fhv']

def data_refresh():
    """Download latest taxi trip data from Official NYC Taxi external URL and record filenames."""
    master_file_record = [] #make a empty list to record what files being downloaded
    for taxi in taxis:
        current_year = start_year  # Reset current_year for each taxi
        while current_year <= end_year:
            for i in range(1, 13):
                if i < 10:
                    month_str = f'0{i}'
                else:
                    month_str = f'{i}'
                
                parquet_name = f'{taxi}_tripdata_{current_year}-{month_str}.parquet'
                
                response = requests.head(f"https://d37ci6vzurychx.cloudfront.net/trip-data/{parquet_name}")
                
                if response.status_code == 403:
                    print(f"Maybe all data up to the latest have been refreshed for {taxi} taxi")
                    break  # Break out of the month loop, but continue with other taxis
                
                # Initiate the download
                urllib.request.urlretrieve(f"https://d37ci6vzurychx.cloudfront.net/trip-data/{parquet_name}", parquet_name)
                print(f'Download Successful: {parquet_name}')
                master_file_record.append(parquet_name)
            
            if response.status_code == 403:
                break  # Break out of the year loop if 403 encountered
            
            current_year += 1

    # Exports a CSV file with records of all files downloaded
    with open('master_file_record.csv', mode='w', newline='') as file:
        df=pd.DataFrame(master_file_record)
        df.to_csv('master_file_record.csv',header=False)

def aggregate_remove_data():
    """Aggregate downloaded Parquet files, remove original files, and record aggregated filenames."""

    # Check if the master file record exists and read it
    if os.path.isfile("./master_file_record.csv"):
        with open('master_file_record.csv', newline='') as csv_file:
            df = pd.read_csv("master_file_record.csv",header=None)
            master_file_record = df[1].values.tolist()
            print(master_file_record)

    # Loop Thorugh Each yeat to aggregate the data 
    for taxi in taxis:
        current_year = start_year  # Reset current_year for each taxi
        while current_year <= end_year:
            duckdb.sql(f"COPY (SELECT * FROM read_parquet('{taxi}_tripdata_{current_year}*.parquet')) TO '{taxi}_tripdata_{current_year}.parquet' (FORMAT PARQUET);")
            current_year += 1
    
    # Iterate over the list of file names and delete each file
    for file_name in master_file_record:
        print(master_file_record)
        os.remove(f"{os.getcwd()}/{file_name}")

    # Get names of the aggregated files    
    master_file_record=glob.glob("*.parquet")

    # Exports a CSV file with records of all files aggregated
    with open('master_file_record.csv', mode='w', newline='') as file:
        df=pd.DataFrame(master_file_record)
        df.to_csv('master_file_record.csv',header=False)

def insert_data_pg():
    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    
    # Check if the master file record exists and read it
    if os.path.isfile("./master_file_record.csv"):
        with open('master_file_record.csv', newline='') as csv_file:
            df = pd.read_csv("master_file_record.csv",header=None)
            master_file_record = df[1].values.tolist()

    for file_name in master_file_record:
        df=pd.read_parquet(file_name,engine="fastparquet")
        df.columns = [c.lower() for c in df.columns]
        file_name = file_name.strip('.parquet')
        df.head(10).to_sql(name=file_name,con=engine,if_exists='replace')

    # Iterate over the list of file names and delete each file
    for file_name in master_file_record:
        print(master_file_record)
        os.remove(f"{os.getcwd()}/{file_name}")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="nyc_to_postgres",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    
    refresh_data = PythonOperator(
        task_id="Refresh_Dataset",
        python_callable=data_refresh
    )

    compress_data = PythonOperator(
        task_id="Aggregating_Dataset",
        python_callable=aggregate_remove_data
    )

    insert_data = PythonOperator(
        task_id="Inserting_values_Postgres",
        python_callable=insert_data_pg
    )


refresh_data>>compress_data>>insert_data