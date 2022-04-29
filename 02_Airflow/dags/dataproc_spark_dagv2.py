# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.dataproc_operator import DataprocCreateClusterOperator, \
#    DataprocClusterDeleteOperator, ClusterGenerator,DataProcSparkOperator
from airflow.providers.google.cloud.operators.dataproc import   ClusterGenerator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator, \
                                                                DataprocSubmitJobOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
REGION = "us-central1"
path = "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"
PYSPARK_URI_LOC=f'gs://{BUCKET}/dataproc/spark_sql_dataproc_v2.py'

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'traffic_crash_data')

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

default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG('dataproc_example_dag',
          #schedule_interval=timedelta(days=1),
          default_args=default_args)

with dag:
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

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    #start >> create_cluster >> run_job
    #start >> run_job
    #run_job >> delete_cluster >> end
    create_cluster_operator_task >> pyspark_task >>delete_cluster >> end