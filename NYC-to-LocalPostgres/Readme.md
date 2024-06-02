# NYC Taxi Trips Data Pipeline with Apache Airflow and Docker

This project sets up a Dockerized Apache Airflow workflow to download, process, and store NYC taxi trip data in a local PostgreSQL database. The workflow is designed to be resource-efficient, making it suitable for running on systems with limited compute power and storage.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Setup Instructions](#setup-instructions)
- [Airflow DAG Workflow](#airflow-dag-workflow)
- [Usage](#usage)
- [Feedback and Improvements](#feedback-and-improvements)

## Project Overview

The workflow pulls taxi trip data from the NYC Taxi & Limousine Commission's website, processes it using DuckDB, and stores it in a local PostgreSQL database. **The Airflow container is based on a trimmed-down version of the official Airflow Docker Compose file to reduce space and resource usage.** The data pipeline includes the following steps:

1. **Download Data:** Downloads trip data from a specified year to the current year.
2. **Aggregate Data:** Aggregates the data year-wise for each taxi type using DuckDB.
3. **Store Data:** Inserts the aggregated data into PostgreSQL tables.
4. **Cleanup:** Deletes intermediate files to save space and updates a master record CSV for tracking.

## Technologies Used

- **PostgreSQL:** Database to store the aggregated trip data.
- **PgAdmin:** Database management tool to interact with PostgreSQL.
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

2. **Set Up Environment Variables:**

   ```sh
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

   Edit the `.env` file and add the following configuration:

   ```env
   # Custom
   COMPOSE_PROJECT_NAME=de-zoomcamp

   # Postgres
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   POSTGRES_DB=airflow

   # Airflow
   AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=10

   AIRFLOW_CONN_METADATA_DB=postgres+psycopg2://airflow:airflow@postgres:5432/airflow
   AIRFLOW_VAR__METADATA_DB_SCHEMA=airflow
   AIRFLOW__CORE__TEST_CONNECTION=Enabled

   _AIRFLOW_WWW_USER_CREATE=True
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow
   ```

3. **Build Docker Containers:**

   ```sh
   docker-compose build
   ```

4. **Start Docker Containers:**

   ```sh
   docker-compose up
   ```

5. **Access Services:**

   - **Airflow UI:** [http://localhost:8080](http://localhost:8080)
     - Username: `airflow`
     - Password: `airflow`
   - **PgAdmin:** [http://localhost:8888](http://localhost:8888)
   - **PostgreSQL:** Access via `pgcli`:

     ```sh
     pgcli -h localhost -p 5432 -u airflow
     ```

## Airflow DAG Workflow

1. **Download Data Task:** Downloads monthly trip data from the specified year to the current year.
2. **Aggregate Data Task:** Uses DuckDB to aggregate the downloaded data year-wise and for each taxi type. Deletes the monthly files after aggregation to save space.
3. **Store Data Task:** Inserts the aggregated data into PostgreSQL tables. Deletes the aggregated files after insertion to save space.
4. **Master Record Update Task:** Updates the `master_file_record.csv` to track downloaded, processed, and missing files.

## Usage

If you want to run the project, please follow the setup instructions above. Once the services run, you can manage and monitor the ETL workflow through the Airflow UI. The PostgreSQL database can be accessed using pgAdmin or `pgcli` for further analysis and verification of the data.

## Feedback and Improvements

This project is a work in progress with room for improvements and bug fixes. I would greatly appreciate any feedback. Future enhancements may include additional data processing steps, better error handling, and workflow optimization. Feel free to contribute to the project by opening issues or submitting pull requests. Your feedback and suggestions are always welcome!
