import json
import pandas as pd
from utils.firehose_pipe import send_firehose_batch
from utils.api import get_crypto_data, get_crypto_price_historical

def _get_data(crypto_to_ingest, intervals, proxy_id):
  ds_row = {}
  for start_dt, end_dt in zip(intervals[:-1], intervals[1:]):
    #print(start_dt, end_dt)
    response = get_crypto_price_historical(crypto_to_ingest, "usd", proxy_id, start_dt, end_dt)
    #print(response)
    for metric, values in response.items():
      for value in values:
        #print(value)
        if value[0] not in ds_row.keys():
          ds_row[value[0]] = {"timestamp": value[0], metric: str(value[1])}
        else:
          ds_row[value[0]].update({metric: str(value[1])})
  return ds_row

def get_data_formated(crypto_to_ingest, intervals, proxy_id, firehose_client):

  out_data = []
  
  info_crypto = get_crypto_data(crypto_to_ingest)

  print(f" reading data for {crypto_to_ingest} - {info_crypto} ")

  ds_row = _get_data(crypto_to_ingest, intervals, proxy_id)

  print(f" extracted historical prices for {crypto_to_ingest} ")

  for ts, v in ds_row.items():
    # ds_row = {ts : {metric1 : [value], metric2 : [value]}}} }}
    price_and_info = {**v, **info_crypto}

    out_data.append(price_and_info)
    #send_message_to_kafka("crypto_prices", payload_to_kafka)
  # desactivate index column
  df = pd.DataFrame(out_data)
  # cast column timestamp of string to datetime
  #df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
  
  df['year'] = df['timestamp'].apply(lambda x: str(pd.to_datetime(x, unit='ms').year))
  df['month'] = df['timestamp'].apply(lambda x: str(pd.to_datetime(x, unit='ms').month).zfill(2))
  df['day'] = df['timestamp'].apply(lambda x: str(pd.to_datetime(x, unit='ms').day).zfill(2))
  df['hour'] = df['timestamp'].apply(lambda x: str(pd.to_datetime(x, unit='ms').hour).zfill(2))
  print(f" numbers of rows to ingest: {df.shape[0]}")

  out_data_to_firehose = []

  # iterate row by row and get each chunk of rows
  for i, row in df.iterrows():

    out_data_to_firehose.append({"Data": (json.dumps(row.to_dict()))})

    if i+1 % 50 == 0:

      send_firehose_batch(out_data_to_firehose, firehose_client)

      out_data_to_firehose = []

  if len(out_data_to_firehose) > 0:

    send_firehose_batch(out_data_to_firehose, firehose_client)

  return df