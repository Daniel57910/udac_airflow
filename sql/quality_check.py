from airflow.hooks.postgres_hook import PostgresHook
import logging

def execute_data_quality_checks(query, tables):
  logger = logging.getLogger(__name__)
  pg_hook = PostgresHook('redshift_lake')

  for table in tables:
    statement = query + table
    result = pg_hook.get_first(statement)
    if result is None:
      raise Exception(f'Load of data into table {table} failed, please review')

  return True

  
