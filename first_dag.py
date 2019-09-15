from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow_tasks.s3_to_gzip import s3_to_gzip
from sql.create_tables import table_commands
from sql.table_definitions import song_staging_columns, log_staging_columns, d_artist_columns, d_song_columns, d_timestamp_columns
from lib.helper_functions import destroy_and_create_schema, s3_to_redshift
from lib.create_dimension_table import create_dimension_table
from lib.create_timestamp_table import create_d_timestamp_table
from datetime import datetime

IAM_ROLE = 'arn:aws:iam::774141665752:role/redshift_s3_role'
PROJECT_PATH = '/Users/danielwork/Documents/GitHub/udac_airflow'
default_args = {
  'owner': 'udacity',
  'start_date': datetime(2018, 1, 12),
  'retries': 0
}

dag = DAG(
  'sparkify_dag', default_args=default_args, description='First Dag', schedule_interval='@monthly')


# song_staging_sync = PythonOperator(
  # task_id='sync_song_staging_from_s3',
  # dag=dag,
  # python_callable=s3_to_gzip,
  # op_kwargs = {'data_type': 'song_data', 'columns': song_staging_columns}
# )
#  
# log_staging_sync = PythonOperator(
  # task_id='sync_log_staging_from_s3',
  # dag=dag,
  # python_callable=s3_to_gzip,
  # op_kwargs = {'data_type': 'log_data', 'columns': log_staging_columns}
# )
# 
# sync_staging_directory_to_s3 = BashOperator(
  # task_id='sync_staging_directory_to_s3',
  # bash_command=f'aws s3 sync {PROJECT_PATH}/data s3://sparkify-airflow-data-2/',
  # dag=dag
#  )
# 
# populate_song_staging_table = PythonOperator(
  # task_id='populate_song_staging_table',
  # dag=dag,
  # python_callable=s3_to_redshift,
  # op_kwargs = {'table_name': 'song_staging', 'data': 'song_data.gz', 'IAM_ROLE': IAM_ROLE}
# )
# 
# populate_log_staging_table = PythonOperator(
  # task_id='populate_log_staging_table',
  # dag=dag,
  # python_callable=s3_to_redshift,
  # op_kwargs = {'table_name': 'log_staging', 'data': 'log_data.gz', 'IAM_ROLE': IAM_ROLE}
# )
# 
# create_schema = PythonOperator(
  # task_id='create_schema',
  # dag=dag,
  # python_callable=destroy_and_create_schema
# )
 
create_d_artist_table = PythonOperator(
  task_id='create_d_artist_table',
  dag=dag,
  python_callable=create_dimension_table,
  op_kwargs = {
    'table_name': 'd_artist', 
    'staging_file': PROJECT_PATH + '/data/song_data.csv', 
    'columns': d_artist_columns,
    'index_columns': ['artist_id'],
    'hashable_columns': ['artist_id', 'artist_name'],
    'disk_path': PROJECT_PATH + '/dimensions'
    }
)

create_d_song_table = PythonOperator(
  task_id='create_d_song_table',
  dag=dag,
  python_callable=create_dimension_table,
  op_kwargs = {
    'table_name': 'd_song', 
    'staging_file': PROJECT_PATH + '/data/song_data.csv', 
    'columns': d_song_columns,
    'index_columns': ['song_id', 'title', 'artist_id'],
    'hashable_columns': ['song_id', 'artist_id'],
    'disk_path': PROJECT_PATH + '/dimensions'
    }
)

create_d_timestamp_table = PythonOperator(
  task_id='create_d_timestamp_table',
  dag=dag,
  python_callable=create_d_timestamp_table,
  op_kwargs = {
    'table_name': 'd_timestamp', 
    'staging_file': PROJECT_PATH + '/data/log_data.csv', 
    'columns': ['ts'],
    'transform_columns': d_timestamp_columns,
    'disk_path': PROJECT_PATH + '/dimensions'
    }
)



# create_schema >> populate_log_staging_table
# create_schema >> populate_song_staging_table
# song_staging_sync >> sync_staging_directory_to_s3
# log_staging_sync >> sync_staging_directory_to_s3
# sync_staging_directory_to_s3 >> populate_song_staging_table
# sync_staging_directory_to_s3 >> populate_log_staging_table

# create_d_artist_table
# create_d_song_table
create_d_timestamp_table