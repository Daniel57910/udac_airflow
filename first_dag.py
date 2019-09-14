from airflow.plugins_manager import AirflowPlugin
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from airflow import DAG
from datetime import datetime
from airflow_tasks.s3_to_gzip import s3_to_gzip
from sql.create_tables import table_commands
import os
import logging
import configparser
import re

IAM_ROLE = 'arn:aws:iam::774141665752:role/redshift_s3_role'

def destroy_and_create_schema():
  logger = logging.getLogger(__name__)
  pg_hook = PostgresHook('redshift_lake')

  for command in table_commands:
    pg_hook.run(command)  

def s3_to_redshift(table_name, data, IAM_ROLE):
  logger = logging.getLogger(__name__)
  pg_hook = PostgresHook('redshift_lake')
  copy_query = "COPY {} FROM 's3://sparkify-airflow-data-2/{}' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(table_name, data, IAM_ROLE)

  pg_hook.run(copy_query)

PROJECT_PATH='/Users/danielwork/Documents/GitHub/udac_airflow'
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 12),
    'retries': 0
}

dag = DAG(
  'sparkify_dag', default_args=default_args, description='First Dag', schedule_interval='@monthly')

redshift_hook_dag = DAG(
  'redshift_hook_dag', default_args=default_args, description='First Dag', schedule_interval='@monthly')


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
  bash_command=f'aws s3 sync {PROJECT_PATH}/data s3://sparkify-airflow-data-2/',
  dag=dag
)

populate_song_staging_table = PythonOperator(
  task_id='populate_song_staging_table',
  dag=dag,
  python_callable=s3_to_redshift,
  op_kwargs = {'table_name': 'song_staging', 'data': 'song_data.gz', 'IAM_ROLE': IAM_ROLE}
)

create_schema = PythonOperator(
  task_id='create_schema',
  dag=dag,
  python_callable=destroy_and_create_schema
)

song_staging_sync >> sync_staging_directory_to_s3
log_staging_sync >> sync_staging_directory_to_s3
sync_staging_directory_to_s3 >> populate_song_staging_table
create_schema >> populate_song_staging_table