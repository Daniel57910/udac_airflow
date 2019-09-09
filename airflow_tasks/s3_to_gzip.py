import pandas as pd
import subprocess
import os
from lib.file_finder import FileFinder
from lib.data_loader import DataLoader
import pandas as pd
import logging

def s3_to_gzip(data_type):
  logger = logging.getLogger(__name__)
  local_path = os.getcwd() + f'/tmp/{data_type}/'
  #subprocess.run(f'aws s3 sync s3://udacity-dend/{data_type} {local_path}', shell=True, check=True)

  file_finder = FileFinder(local_path, '*.json')
  file_names = list(file_finder.return_file_names())

  data_loader = DataLoader(file_names)

  dataframe = data_loader.create_dataframe_from_files()

  dataframe.to_csv(
    f'/Users/danielwork/Documents/GitHub/udac_airflow/data/{data_type}.gz',
    header=True,
    index=False,
    compression='gzip'
  )

