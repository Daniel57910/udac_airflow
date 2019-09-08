import pandas as pd
import subprocess
import os

def s3_to_gzip(data_type):
  local_path = os.getcwd() + f'/tmp/{data_type}'
  subprocess.run(f'aws s3 sync s3://udacity-dend/{data_type} {local_path}', shell=True, check=True)

