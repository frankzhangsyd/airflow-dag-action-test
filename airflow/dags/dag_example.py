import datetime
import logging
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.tableau.operators.tableau import TableauOperator
from airflow.providers.tableau.sensors.tableau import TableauJobStatusSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

#Get variables from Airflow variables
ENV = models.Variable.get('ENV')

# Set environment. Either DEV or PROD
if ENV == "dev":
    target = "dev-project"

elif ENV == "prod":
    target = "prod-project"
else:
    raise ValueError(f"Invalid environment: {ENV}")

default_dag_args = {
    'start_date': datetime.now(),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'test@test.com',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dag_example',
    schedule_interval=None,
    default_args=default_dag_args,
    catchup=False
)

start = EmptyOperator(task_id='start', dag=dag)
sample_task1 = BigQueryInsertJobOperator(
    task_id='sample_task1',
    configuration={
        "query": {
            "query": "SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10",
            "destinationTable": {
                "projectId": target,
                "datasetId": "test_dataset", 
                "tableId": "test_table"
            },
            "bigquery_conn_id": "bigquery_default",            
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER"
        }
    },
    dag=dag)

sample_task2 = TableauOperator(
    resource='datasources',
    method='refresh',
    find='sample_datasource',
    match_with='id',
    blocking_refresh=False,
    task_id='sample_task2',
    dag=dag
    )
start >> sample_task1 >> sample_task2