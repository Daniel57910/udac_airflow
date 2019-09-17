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

def s3_to_gzip(data_type, columns):
  logger = logging.getLogger(__name__)
  local_path = os.getcwd() + f'/tmp/{data_type}/'

  if not os.path.exists(local_path):
    os.makedirs(local_path)

  subprocess.run(f'aws s3 sync s3://udacity-dend/{data_type} {local_path}', shell=True, check=True)

  file_finder = FileFinder(local_path, '*.json')
  file_names = list(file_finder.return_file_names())

  data_loader = DataLoader(file_names)

  dataframe = data_loader.create_dataframe_from_files()

  clean_dataframe_of_non_alphanumeric_characters(dataframe, columns)

  dataframe.to_csv(
    f'/Users/danielwork/Documents/GitHub/udac_airflow/data/{data_type}.gz',
    header=False,
    index=False,
    compression='gzip'
  )

  dataframe.to_csv(
    f'/Users/danielwork/Documents/GitHub/udac_airflow/data/{data_type}.csv',
    header=True,
    index=False
  )
