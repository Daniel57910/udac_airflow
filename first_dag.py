from airflow.plugins_manager import AirflowPlugin
from airflow.operators.python_operator import PythonOperator
import logging
from airflow import DAG
from datetime import datetime
from airflow_tasks.s3_to_gzip import s3_to_gzip

# class HelloWorldOperator(BaseOperator):
#   logger = logging.getLogger(__name__)
#   def __init__(self, *args, **kwargs):
#     super(HelloWorldOperator, self).__init__(*args, **kwargs)
  
#   def execute(self):
#     self.logger.info('HELLO FIRST DAG')

def second_callable():
  logger = logging.getLogger(__name__)
  logger.info("FIRST DAG COMPLETED")
  return 'FIRST DAG COMPLETED'

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

song_staging_sync
log_staging_sync