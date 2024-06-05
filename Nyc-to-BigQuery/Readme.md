# NYC Taxi Trips Data Pipeline with Apache Airflow, Docker and GCP

This project sets up a Dockerized Apache Airflow workflow to download, process, and store NYC taxi trip data in a Google Cloud Service and Bigquery. The workflow is designed to be resource-efficient, making it suitable for running on systems with limited compute power and storage.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Setup Instructions](#setup-instructions)
- [Airflow DAG Workflow](#airflow-dag-workflow)
- [Usage](#usage)
- [Feedback and Improvements](#feedback-and-improvements)

## Project Overview

The workflow pulls taxi trip data from the NYC Taxi & Limousine Commission's website, processes it using DuckDB, and stores it in a Google cloud storage and then into Bigquery database. The data pipeline includes the following steps:

1. **Download Data:** Downloads trip data from a specified year to the current year.
2. **Aggregate Data:** Aggregates the data year-wise for each taxi type using DuckDB.
3. **Store Data:** Inserts the aggregated data into Google Cloud Storage.
3. **Transfer Data:** Inserts the aggregated data as tables into Google Bigquery.
4. **Cleanup:** Deletes intermediate files to save space and updates a master record CSV for tracking.

## Technologies Used

- **Google Cloud Storage:** Bucket to store the aggregated trip data.
- **Google Bigquery:** Online Query engine service provided by google.
- **Apache Airflow:** Workflow orchestration tool to manage the ETL pipeline.
- **DuckDB:** An analytical database aggregates trip data.
- **Docker:** Containerization technology to deploy the workflow.
- **Python:** Programming language used in the Airflow DAG.
- **SQL:** Query language for database operations.

## Setup Instructions

1. **Create Required Directories:**

   ```sh
   mkdir -p ./logs ./plugins ./config
   ```

2. **Create a folder for google service account key**
    ```sh
    .google/credntials/google_credentials.json
    ```

3. **Set Up Environment Variables:**

   ```sh
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

   Edit the `.env` file and add the following configuration:

   ```env
   #Custom
    COMPOSE_PROJECT_NAME=de-zoomcamp
    GOOGLE_APPLICATION_CREDENTIALS=/.google/credentials/google_credentials.json
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json
    AIRFLOW_UID=<airflow_uid>
    GCP_PROJECT_ID=<gcp_project_id>
    GCP_GCS_BUCKET=<gcs_bucket_name>
    
    # Postgres
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_DB=airflow
    
    # Airflow
    AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=10
    
    AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    AIRFLOW_CONN_METADATA_DB=postgres+psycopg2://airflow:airflow@postgres:5432/airflow
    AIRFLOW_VAR__METADATA_DB_SCHEMA=airflow
    AIRFLOW__CORE__TEST_CONNECTION=Enabled
   ```

4. **Build Docker Containers:**

   ```sh
   docker-compose build
   ```
5. ** create a scripts folder and add a file named "entrypoint.sh". Then enter the following code into it.
    ```sh
    #!/usr/bin/env bash
    export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
    export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=${AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT}
    
    airflow db upgrade
    
    airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
    # "$_AIRFLOW_WWW_USER_USERNAME" -p "$_AIRFLOW_WWW_USER_PASSWORD"
    
    airflow webserver
    ```

6. **Start Docker Containers:**

   ```sh
   docker-compose up
   ```

7. **Access Services:**

   - **Airflow UI:** [http://localhost:8080](http://localhost:8080)
     - Username: `airflow`
     - Password: `airflow`

## Airflow DAG Workflow

1. **Download Data Task:** Downloads monthly trip data from the specified year to the current year.
2. **Aggregate Data Task:** Uses DuckDB to aggregate the downloaded data year-wise and for each taxi type. Deletes the monthly files after aggregation to save space.
3. **Store Data Task:** Inserts the aggregated data into Google Cloud Storage. Transfers the files as tables into Bigquery. Deletes the aggregated files after insertion to save space.
4. **Master Record Update Task:** Updates the `master_file_record.csv` to track downloaded, processed, and missing files.

## Usage

If you want to run the project, please follow the setup instructions above. Once the services run, you can manage and monitor the ETL workflow through the Airflow UI. Check the Google cloud platform to check Cloud Storage and Bigquery.

## Feedback and Improvements

This project is a work in progress with room for improvements and bug fixes. I would greatly appreciate any feedback. Future enhancements may include additional data processing steps, better error handling, and workflow optimization. Feel free to contribute to the project by opening issues or submitting pull requests. Your feedback and suggestions are always welcome!
