import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import storage
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'traffic_crash_data')

url_base = f"https://data.cityofchicago.org/api/views"
url_end = f"rows.csv?accessType=DOWNLOAD"
DATASET = "traffic_crash_data"
FILES_RANGE= {'crash' : '85ca-t3if', 'vehicles' : '68nd-jvt3','people' : 'u6pd-qa9d'}

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 1,
}
# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="local_2_gcs_2_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-capstone'],
) as dag:
    for FTYPE, FNAME in FILES_RANGE.items():
        download_dataset_task = BashOperator(
            task_id=f"download_{FTYPE}_dataset_task",
            bash_command=f"curl -sS {url_base}/{FNAME}/{url_end} > {path_to_local_home}/{FNAME}.csv"
        )
        format_to_parquet_task = PythonOperator(
            task_id=f"format_{FTYPE}_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{FNAME}.csv",
            },
        )
        local_to_gcs_task = PythonOperator(
            task_id=f"{FTYPE}_local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{FTYPE}/{FNAME}.parquet",
                "local_file": f"{path_to_local_home}/{FNAME}.parquet",
            },
        )
        #bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        #    task_id=f"{FTYPE}_bigquery_external_table_task",
        #    table_resource={
        #        "tableReference": {
        #            "projectId": PROJECT_ID,
        #            "datasetId": BIGQUERY_DATASET,
        #            "tableId": f"{FTYPE}_table",
        #        },
        #        "externalDataConfiguration": {
        #            "sourceFormat": "PARQUET",
        #            "sourceUris": [f"gs://{BUCKET}/raw/{FTYPE}/{FNAME}.parquet"],
        #        },
        #    },
        #)
        delete_dataset_task = BashOperator(
            task_id=f"delete_{FTYPE}_dataset_task",
            bash_command=f"rm {path_to_local_home}/{FNAME}.csv {path_to_local_home}/{FNAME}.parquet"
        )
        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_dataset_task