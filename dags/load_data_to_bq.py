import logging
import json
import requests
import csv
from airflow import DAG
from airflow.utils import timezone
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

from google.cloud import storage, bigquery
from google.oauth2 import service_account

# Define constant variables
DAGS_FOLDER = "/opt/airflow/dags"
DATA_FOLDER = "/opt/airflow/data"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "data-engineer-bootcamp-384606"
GCS_BUCKET = "personal-practice-networkrail-bootcamp"
BIGQUERY_DATASET = "personal_practice_networkrail_bootcamp"
KEYFILE = f"{DAGS_FOLDER}/data-engineer-bootcamp-384606-0cab607d41e8.json"
DATASET = Dataset("gcs://personal-practice-networkrail-bootcamp/networkrail/movements")

def _upload_from_gcs_to_bq(**context):
    ds = context['data_interval_start'].to_date_string()

    storage_service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(storage_service_account_info)
    storage_client = storage.Client(
        credentials=credentials,
        project=PROJECT_ID,
        # location=LOCATION,
    )
    
    bucket_cursor = storage_client.bucket(GCS_BUCKET)

    source_path = f'{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}-{ds}.csv'
    blob = bucket_cursor.blob(source_path)

    if (blob.exists()):
        bq_service_account_info = json.load(open(KEYFILE))
        credentials = service_account.Credentials.from_service_account_info(bq_service_account_info)
        bq_client = bigquery.Client(
            credentials=credentials,
            project=PROJECT_ID,
            # location=LOCATION,
        )   

        table_schema = [
            bigquery.SchemaField("event_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("gbtt_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("original_loc_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("planned_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("timetable_variation", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("original_loc_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("current_train_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("delay_monitoring_point", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("next_report_run_time", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("reporting_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("actual_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("correction_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("event_source", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_file_address", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("platform", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("division_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_terminated", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("train_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("offroute_ind", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("variation_status", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("train_service_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("toc_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("loc_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("auto_expected", bigquery.enums.SqlTypeNames.BOOLEAN),
            bigquery.SchemaField("direction_ind", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("route", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("planned_event_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("next_report_stanox", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("line_ind", bigquery.enums.SqlTypeNames.STRING),
        ]
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            schema=table_schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='actual_timestamp'
            )
        )

        partition_identifier = ds.replace('-', '')
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{DATA}${partition_identifier}"
        job = bq_client.load_table_from_uri(
            f"gs://{GCS_BUCKET}/{source_path}",
            table_id,
            job_config=job_config,
            location=LOCATION,
        )
        job.result()

        fetched_table = bq_client.get_table(table_id)
        msg = f"Loaded {fetched_table.num_rows} rows and " \
            f"{len(fetched_table.schema)} columns to {table_id}"
        logging.info(msg)



default_args = {
    "owner": "slothPete7773",
    "start_date": timezone.datetime(2023, 5, 1)
}
with DAG(
    dag_id="load_data_to_bq",
    schedule=[DATASET],
    default_args=default_args,
    max_active_runs=3,
    catchup=False,
    tags=['EndProject'],
):  
    start = EmptyOperator(
        task_id="start",
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
    # extract_data_from_postgres >> upload_to_gcs >> upload_from_gcs_to_bq >> end
    # extract_data_from_postgres >> do_nothing >> end