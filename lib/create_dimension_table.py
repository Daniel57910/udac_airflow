import pandas as pd
import logging
import os
def create_dimension_table(table_name, staging_file, columns, index_columns, hashable_columns, disk_path):

  if not os.path.exists(disk_path):
    os.makedirs(disk_path)

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
  
