import pandas as pd
import logging

def create_dimension_table(table_name, staging_file, columns, index_columns, hashable_columns, disk_path):
  logger = logging.getLogger(__name__)

  logger.info(locals())
    
  dataframe = pd.read_csv(staging_file)
  dataframe = dataframe[columns]
  dataframe.drop_duplicates(index_columns, keep='last', inplace=True)
  dataframe.dropna(subset=hashable_columns, inplace=True)

  dataframe.to_csv(
    disk_path + f'/{table_name}.gz',
    header=False,
    index=False,
    compression='gzip'
  )
  
  dataframe.to_csv(
    disk_path + f'/{table_name}.csv',
    header=True,
    index=False
  )
