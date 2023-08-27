import pandas as pd
from utils.api import get_crypto_data, get_crypto_price_historical


def _get_data(crypto_to_ingest, intervals):
  ds_row = {}
  for start_dt, end_dt in zip(intervals[:-1], intervals[1:]):
    #print(start_dt, end_dt)
    response = get_crypto_price_historical(crypto_to_ingest, "usd", start_dt, end_dt)
    print(response)
    for metric, values in response.items():
      for value in values:
        #print(value)
        if value[0] not in ds_row.keys():
          ds_row[value[0]] = {"timestamp": value[0], metric: str(value[1])}
        else:
          ds_row[value[0]].update({metric: str(value[1])})
  return ds_row

def get_data_formated(crypto_to_ingest, intervals):

  out_data = []
  
  info_crypto = get_crypto_data(crypto_to_ingest)

  ds_row = _get_data(crypto_to_ingest, intervals)
    
  for ts, v in ds_row.items():
    #print("ingesting: ", ts)
    # ds_row = {ts : {metric1 : [value], metric2 : [value]}}} }}
    out_data.append({**v, **info_crypto})
    #print(out_data)

  # desactivate index column
  df = pd.DataFrame(out_data)
  # cast column timestamp of string to datetime
  df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
  
  df['year'] = df['timestamp'].apply(lambda x: str(x.year))
  df['month'] = df['timestamp'].apply(lambda x: str(x.month).zfill(2))
  df['day'] = df['timestamp'].apply(lambda x: str(x.day).zfill(2))
  df['hour'] = df['timestamp'].apply(lambda x: str(x.hour).zfill(2))
  print(f" numbers of rows to ingest: {df.shape[0]}")
  return df