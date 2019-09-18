import pandas as pd
import subprocess
import os
from lib.file_finder import FileFinder
from lib.data_loader import DataLoader
import pandas as pd
import logging
import re
import os

def clean_dataframe_of_non_alphanumeric_characters(dataframe, columns):
  '''
  cleans dataframe columns of non alphanumeric or whitespace to ensure they can be entered into DB
  '''
  for col in columns:
    dataframe[col] = dataframe[col].apply(remove_dangerous_characters)

def remove_dangerous_characters(entry):
  '''
  regex that returns all alphanumeric and whitespace characters from a string
  '''
  regex = r'([^\s\w\d.]+)'
  entry =  re.sub(regex, '', str(entry))
  if entry == 'nan':
    return None
  
  return entry

def s3_to_gzip(data_type, columns, path):
  logger = logging.getLogger(__name__)

  temp_path = path + f'/tmp/{data_type}'
  staging_path = path + '/staging'
  logger.info(f'Syncing {data_type} to {temp_path}')

  if not os.path.exists(temp_path):
    os.makedirs(temp_path)

  subprocess.run(f'aws s3 sync s3://udacity-dend/{data_type} {temp_path}', shell=True, check=True)

  file_finder = FileFinder(temp_path+ '/', '*.json')
  file_names = list(file_finder.return_file_names())

  data_loader = DataLoader(file_names)

  dataframe = data_loader.create_dataframe_from_files()

  clean_dataframe_of_non_alphanumeric_characters(dataframe, columns)

  logger.info(f'saving {data_type} staging file to {staging_path}')
  if not os.path.exists(staging_path):
    os.makedirs(staging_path)

  dataframe.to_csv(
    staging_path + f'/{data_type}.gz',
    header=False,
    index=False,
    compression='gzip'
  )

  dataframe.to_csv(
    staging_path + f'/{data_type}.csv',
    header=True,
    index=False
  )
