import logging
from airflow.hooks.postgres_hook import PostgresHook
import subprocess

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
  pg_hook = PostgresHook('redshift_lake')

  for table in tables:
    copy_query = "COPY {} FROM 's3://sparkify-airflow-data-2/{}.gz' iam_role '{}' region 'eu-west-2' gzip delimiter ',';".format(table, table, IAM_ROLE)
    pg_hook.run(copy_query)


