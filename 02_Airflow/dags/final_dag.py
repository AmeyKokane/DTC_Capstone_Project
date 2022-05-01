import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import   ClusterGenerator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator, \
                                                                DataprocSubmitJobOperator
from google.cloud import storage
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
REGION = "us-central1"
path = "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"
PYSPARK_URI_LOC=f'gs://{BUCKET}/dataproc/spark_sql_dataproc_v3.py'

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'traffic_crashes_data')

url_base = f"https://data.cityofchicago.org/api/views"
url_end = f"rows.csv?accessType=DOWNLOAD"
DATASET = "traffic_crash_data"
FILES_RANGE= {'crash' : '85ca-t3if', 'vehicles' : '68nd-jvt3','people' : 'u6pd-qa9d'}

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
            project_id=PROJECT_ID,
            zone="us-central1-f",
            master_machine_type="n1-standard-4",
            master_disk_size=500,
            num_masters=1,
            num_workers=2,                          # single node mode
            idle_delete_ttl=900,                    # idle time before deleting cluster
            init_actions_uris=[path],
            metadata={'PIP_PACKAGES': 'spark-nlp pyyaml requests pandas openpyxl'},
        ).make()
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
    dag_id="local_gcs_sparkdataproc_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-capstone'],
) as dag:

    with TaskGroup(group_id='download_local_and_upload_GCS') as dwldloc_n_upldgcs_tag:
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
    
    with TaskGroup(group_id='sparkdataproc_and_bigquery') as dtproc_n_bq_tag:
        create_cluster_operator_task = DataprocCreateClusterOperator(
            task_id='create_dataproc_cluster',
            cluster_name="dtc-capstone-cluster",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_config=CLUSTER_GENERATOR_CONFIG
        )
        pyspark_job = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": 'dtc-capstone-cluster'},
                "pyspark_job": {
                    "main_python_file_uri": PYSPARK_URI_LOC,
                    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
                    #"properties": {
                    #    "spark.jars.packages":"com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3"
                    #},
                    "args": [
                        f"--input_crashes=gs://{BUCKET}/raw/crash/*",
                        f"--input_vehicles=gs://{BUCKET}/raw/vehicles/*",
                        f"--input_people=gs://{BUCKET}/raw/people/*",
                        f"--output=gs://{BUCKET}/clean"
                        ]
                }
            }

        pyspark_task = DataprocSubmitJobOperator(
                task_id='spark_transform_sparksubmit',
                job=pyspark_job,
                region=REGION,
                project_id=PROJECT_ID,
                trigger_rule='all_done'
            )
        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster", 
            project_id=PROJECT_ID,
            cluster_name="dtc-capstone-cluster", 
            region=REGION
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id='bigquery_external_table_task',
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": 'final_summ_external_table',
                    },
                    #"schema": {
                    #            "fields": [
                    #        {"CRASH_RECORD_ID": "CRASH_RECORD_ID", "type": "STRING"},
                    #        {"RD_NO": "RD_NO", "type": "STRING"},
                    #        {"CRASH_DATE": "CRASH_DATE", "type": "TIMESTAMP"},
                    #        {"CRASH_MONTH": "CRASH_MONTH", "type": "STRING"},
                    #        {"CRASH_YEAR": "CRASH_YEAR", "type": "STRING"},
                    #        {"YEAR_MONTH": "YEAR_MONTH", "type": "STRING"},
                    #        {"DAY_OF_WEEK": "DAY_OF_WEEK", "type": "STRING"},
                    #        {"HOUR_OF_DAY": "HOUR_OF_DAY", "type": "INTEGER"},
                    #        {"POSTED_SPEED_LIMIT": "POSTED_SPEED_LIMIT", "type": "INTEGER"},
                    #        {"WEATHER_CONDITION": "WEATHER_CONDITION", "type": "STRING"},
                    #        {"LIGHTING_CONDITION": "LIGHTING_CONDITION", "type": "STRING"},
                    #        {"FIRST_CRASH_TYPE": "FIRST_CRASH_TYPE", "type": "STRING"},
                    #        {"STREET_NO": "STREET_NO", "type": "INTEGER"},
                    #        {"STREET_DIRECTION": "STREET_DIRECTION", "type": "STRING"},
                    #        {"STREET_NAME": "STREET_NAME", "type": "STRING"},
                    #        {"INJURIES_TOTAL": "INJURIES_TOTAL", "type": "INTEGER"},
                    #        {"INJURIES_FATAL": "INJURIES_FATAL", "type": "INTEGER"},
                    #        {"CNT_ENTITIES_ACCID": "CNT_ENTITIES_ACCID", "type": "INTEGER"},
                    #        {"Cnt_MOTORVEHICLES_ACCID": "Cnt_MOTORVEHICLES_ACCID", "type": "INTEGER"},
                    #        {"VEHICLE1_ID": "VEHICLE1_ID", "type": "INTEGER"},
                    #        {"VEHICLE1_MAKE": "VEHICLE1_MAKE", "type": "STRING"},
                    #        {"VEHICLE1_MODEL": "VEHICLE1_MODEL", "type": "STRING"},
                    #        {"VEHICLE2_ID": "VEHICLE2_ID", "type": "INTEGER"},
                    #        {"VEHICLE2_MAKE": "VEHICLE2_MAKE", "type": "STRING"},
                    #        {"VEHICLE2_MODEL": "VEHICLE2_MODEL", "type": "STRING"},
                    #        {"VEHICLE3_ID": "VEHICLE3_ID", "type": "INTEGER"},
                    #        {"VEHICLE3_MAKE": "VEHICLE3_MAKE", "type": "STRING"},
                    #        {"VEHICLE3_MODEL": "VEHICLE3_MODEL", "type": "STRING"},
                    #        {"VEHICLE4_ID": "VEHICLE4_ID", "type": "INTEGER"},
                    #        {"VEHICLE4_MAKE": "VEHICLE4_MAKE", "type": "STRING"},
                    #        {"VEHICLE4_MODEL": "VEHICLE4_MODEL", "type": "STRING"},
                    #        {"Cnt_Drivers_Accid":"Cnt_Drivers_Accid","type": "INTEGER"},
                    #        {"PERSON1_ID":"PERSON1_ID","type": "INTEGER"},
                    #        {"PERSON1_SEX":"PERSON1_SEX","type": "STRING"},
                    #        {"PERSON1_AGE":"PERSON1_AGE","type": "INTEGER"},
                    #        {"PERSON1_INJURY_CLASSIFICATION":"PERSON1_INJURY_CLASSIFICATION","type": "STRING"},
                    #        {"PERSON2_ID":"PERSON2_ID","type": "INTEGER"},
                    #        {"PERSON2_SEX":"PERSON2_SEX","type": "STRING"},
                    #        {"PERSON2_AGE":"PERSON2_AGE","type": "INTEGER"},
                    #        {"PERSON2_INJURY_CLASSIFICATION":"PERSON2_INJURY_CLASSIFICATION","type": "STRING"},
                    #        {"PERSON3_ID":"PERSON3_ID","type": "INTEGER"},
                    #        {"PERSON3_SEX":"PERSON3_SEX","type": "STRING"},
                    #        {"PERSON3_AGE":"PERSON3_AGE","type": "INTEGER"},
                    #        {"PERSON3_INJURY_CLASSIFICATION":"PERSON3_INJURY_CLASSIFICATION","type": "STRING"},
                    #        {"PERSON4_ID":"PERSON4_ID","type": "INTEGER"},
                    #        {"PERSON4_SEX":"PERSON4_SEX","type": "STRING"},
                    #        {"PERSON4_AGE":"PERSON4_AGE","type": "INTEGER"},
                    #        {"PERSON4_INJURY_CLASSIFICATION":"PERSON4_INJURY_CLASSIFICATION","type": "STRING"},
                    #            ]
                    #},
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/clean/*.parquet"],
                },
            },
        )

        #update_table_schema = BigQueryUpdateTableSchemaOperator(
        ##task_id="update_table_schema",
        ##dataset_id={BIGQUERY_DATASET},
        ##table_id="final_summ_external_table",
        ##schema_fields_updates=[
        ##    {"CRASH_DATE": "emp_name", "description": "Name of employee"},
        ##    {"name": "salary", "description": "Monthly salary in USD"},
        ##],
        #)

        CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.final_summary \
                    AS \
                SELECT * FROM {BIGQUERY_DATASET}.final_summ_external_table;"
            )

            # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        end = DummyOperator(
            task_id='end',
            trigger_rule='one_success'
        )

    #start >> create_cluster >> run_job
    #start >> run_job
    #run_job >> delete_cluster >> end
        create_cluster_operator_task >> pyspark_task >>delete_cluster >> bigquery_external_table_task >> bq_create_partitioned_table_job >> end
    dwldloc_n_upldgcs_tag >> dtproc_n_bq_tag

