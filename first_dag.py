from airflow.plugins_manager import AirflowPlugin
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from airflow import DAG
from datetime import datetime
from airflow_tasks.s3_to_gzip import s3_to_gzip
import os

PROJECT_PATH='/Users/danielwork/Documents/GitHub/udac_airflow'
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 12),
    'retries': 0
}

dag = DAG(
  'sparkify_dag', default_args=default_args, description='First Dag', schedule_interval='@monthly')

song_staging_sync = PythonOperator(
  task_id='sync_song_staging_from_s3',
  dag=dag,
  python_callable=s3_to_gzip,
  op_kwargs = {'data_type': 'song_data'}
)

log_staging_sync = PythonOperator(
  task_id='sync_log_staging_from_s3',
  dag=dag,
  python_callable=s3_to_gzip,
  op_kwargs = {'data_type': 'log_data'}
)

sync_staging_directory_to_s3 = BashOperator(
  task_id='sync_staging_directory_to_s3',
  bash_command=f'aws s3 sync {PROJECT_PATH}/data s3://sparkify-airflow-data/',
  dag=dag
)

song_staging_sync
log_staging_sync

song_staging_sync >> sync_staging_directory_to_s3
log_staging_sync >> sync_staging_directory_to_s3