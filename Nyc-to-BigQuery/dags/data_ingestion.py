import os
import urllib.request
import datetime
import requests
import duckdb
import glob
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from airflow.operators.python import PythonOperator
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

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
        os.remove(f"/opt/airflow/{file_name}")

    # Get names of the aggregated files    
    master_file_record=glob.glob("*.parquet")

    # Exports a CSV file with records of all files aggregated
    with open('master_file_record.csv', mode='w', newline='') as file:
        df=pd.DataFrame(master_file_record)
        df.to_csv('master_file_record.csv',header=False)

def upload_to_gcs():
    """Upload aggregated Parquet files to GCS and remove local copies."""

    # Check if the master file record exists and read it
    if os.path.isfile("./master_file_record.csv"):
        with open('master_file_record.csv', newline='') as csv_file:
            df = pd.read_csv("master_file_record.csv",header=None)
            master_file_record = df[1].values.tolist()

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    for file_name in master_file_record:
        blob = bucket.blob(f"raw/{file_name}")
        blob.upload_from_filename(f"{path_to_local_home}/{file_name}")
    
    # Iterate over the list of file names and delete each file
    for file_name in master_file_record:
        print(master_file_record)
        os.remove(f"/opt/airflow/{file_name}")

prefix="raw" #not neccessary but used as a variable inside next function

def get_parquet_files_from_gcs(**kwargs):
    """List all Parquet files in a GCS bucket under a given prefix."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)
    parquet_files = [os.path.basename(blob.name) for blob in blobs if blob.name.endswith('.parquet')]
    print(parquet_files)
    # Push the list of files to XCom
    kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)

#dataset_id="trips_data" #not needed as the dataset name as been initiliased on top. If gave probs then use this

def load_parquet_to_bigquery(**kwargs):
    """Load a Parquet file from GCS into a BigQuery table with the table name derived from the file name."""
    # Initialize BigQuery client
    bq_client = bigquery.Client()
    parquet_files = kwargs['ti'].xcom_pull(key='parquet_files', task_ids='Getting_FileNames_From_GCS')
    print(parquet_files)
    for parquet_file in parquet_files:
        # Construct the full GCS URI
        gcs_uri = f"gs://{BUCKET}/{prefix}/{parquet_file}"

        # Extract table name from the Parquet file name
        table_name = os.path.splitext(os.path.basename(parquet_file))[0]
        table_id = f"{bq_client.project}.{BIGQUERY_DATASET}.{table_name}"

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True
        )

        # Load data from GCS into BigQuery
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()
        print(f"Loaded {parquet_file} into {table_id}")



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="final_test",
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

    upload_data = PythonOperator(
        task_id="Uploading_Dataset_GCS",
        python_callable=upload_to_gcs
    )

    get_gcs_files = PythonOperator(
        task_id="Getting_FileNames_From_GCS",
        python_callable=get_parquet_files_from_gcs
    )

    create_bq_tables = PythonOperator(
        task_id="Creating_BQ_Tables_From_GCS",
        python_callable=load_parquet_to_bigquery
    )


refresh_data>>compress_data>>upload_data>>get_gcs_files>>create_bq_tables