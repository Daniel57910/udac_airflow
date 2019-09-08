from airflow.plugins_manager import AirflowPlugin
from airflow.operators.python_operator import PythonOperator
import logging
from airflow import DAG
from datetime import datetime

# class HelloWorldOperator(BaseOperator):
#   logger = logging.getLogger(__name__)
#   def __init__(self, *args, **kwargs):
#     super(HelloWorldOperator, self).__init__(*args, **kwargs)
  
#   def execute(self):
#     self.logger.info('HELLO FIRST DAG')

def first_callable():
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

first_task = PythonOperator(
  task_id='first_task',
  dag=dag,
  python_callable=first_callable
)

second_task = PythonOperator(
  task_id='second_task',
  dag=dag,
  python_callable=first_callable
)

first_task 