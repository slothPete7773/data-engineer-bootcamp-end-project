import logging
import json
import requests
import csv
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google.cloud import storage, bigquery
from google.oauth2 import service_account

# Define constant variables
DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "data-engineer-bootcamp-384606"
GCS_BUCKET = "deb-bootcamp-100033"
BIGQUERY_DATASET = "networkrail"
KEYFILE = f"{DAGS_FOLDER}/cred.json"

# Define Tasks
def _extract_data_from_postgres(**context):
    ds = context['data_interval_start'].to_date_string()

    pg_hook = PostgresHook(
        postgres_conn_id="pg_airflow_conn",
        schema="networkrail"
    )

    connection = pg_hook.get_conn()
    pg_cursor = connection.cursor()

    sql_query = f"""
        select *
        from movements
        where date(actual_timestamp)='{ds}' 
    """    
    logging.info(sql_query)

    pg_cursor.execute(sql_query)
    query_result = pg_cursor.fetchall()

    print(f"ds = {ds}")
    # print(f"timezone = {timezone.datetime(2023, 5, 24)}")

    if query_result:
        with open(f"{DAGS_FOLDER}/data/{DATA}-{ds}.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(file)
            headers = [
                "event_type",
                "gbtt_timestamp",
                "original_loc_stanox",
                "planned_timestamp",
                "timetable_variation",
                "original_loc_timestamp",
                "current_train_id",
                "delay_monitoring_point",
                "next_report_run_time",
                "reporting_stanox",
                "actual_timestamp",
                "correction_ind",
                "event_source",
                "train_file_address",
                "platform",
                "division_code",
                "train_terminated",
                "train_id",
                "offroute_ind",
                "variation_status",
                "train_service_code",
                "toc_id",
                "loc_stanox",
                "auto_expected",
                "direction_ind",
                "route",
                "planned_event_type",
                "next_report_stanox",
                "line_ind",
            ]
            
            writer.writerow(headers)        
            for item in query_result:
                writer.writerow(item)
                logging.info(item)
        return "upload_to_gcs"
    else:
        return "do_nothing"

def _upload_to_gcs():
    pass

def _upload_from_gcs_to_bq():
    pass

default_args = {
    "owner": "slothPete7773",
    "start_date": timezone.datetime(2023, 5, 1)
}
with DAG(
    dag_id="load_data_to_gcs_to_bq",
    schedule="@daily",
    default_args=default_args,
    max_active_runs=3,
    catchup=False,
    tags=['EndProject'],
):  
    extract_data_from_postgres = BranchPythonOperator(
        task_id="extract_data_from_postgres",
        python_callable=_extract_data_from_postgres
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs
    )
    
    upload_from_gcs_to_bq = PythonOperator(
        task_id="upload_from_gcs_to_bq",
        python_callable=_upload_from_gcs_to_bq
    )

    do_nothing = EmptyOperator(
        task_id="do_nothing"
    )
    
    end = EmptyOperator(
        task_id="end"
    )
    extract_data_from_postgres >> upload_to_gcs >> upload_from_gcs_to_bq >> end
    extract_data_from_postgres >> do_nothing >> end