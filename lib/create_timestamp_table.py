import pandas as pd
from datetime import datetime 
import logging

def unpack_timestamp(ts):

  logger = logging.getLogger(__name__)
  '''
  receives a timestamp and returns a list of time variables that match the d_timestamp table
  the timestamp is appended to the list as this is used to join d_timestamp on f_songplay
  example:
  ts entered = 1541903636796
  returned = [2018, 11, 11, 33, 56, 2, True, 1541903636796]
  '''

  unpacked_ts = list(datetime.fromtimestamp(int(ts // 1000)).timetuple()[0: 7])
  unpacked_ts[-1] = unpacked_ts[-1] > 5
  return (unpacked_ts + [ts])

def create_d_timestamp_table(table_name, staging_file, columns, disk_path, transform_columns):

  dataframe = pd.read_csv(staging_file)
  dataframe = dataframe[columns]
  dataframe.dropna(subset=columns, inplace=True)

  timestamp_dataframe = pd.DataFrame(
      list(map(
          unpack_timestamp, dataframe['ts'].values
      )),
      columns=transform_columns
  )

  timestamp_dataframe.to_csv(
    disk_path + f'/{table_name}.gz',
    header=False,
    index=False,
    compression='gzip'
  )
  
