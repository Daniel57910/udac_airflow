import logging
from airflow.hooks.postgres_hook import PostgresHook
from sql.create_tables import table_commands
from sql.f_songplay_insert import  f_songplay_insert
import subprocess
from multiprocessing import Pool

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


def s3_to_redshift_parralel(tables, IAM_ROLE):
  logger = logging.getLogger(__name__)
  queries = []

  for table in tables:
    queries.append(
      "COPY {} FROM 's3://sparkify-airflow-data-2/{}.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(table, table, IAM_ROLE)
    )

  with Pool(processes=4) as pool:
    pool.map(execute_postgres_hook, queries)

def execute_postgres_hook(query):
   pg_hook = PostgresHook('redshift_lake')
   pg_hook.run(query)
