import os
import boto3
import requests
import pandas as pd
import awswrangler as wr
from pprint import pprint

suffix = "_me"
session = boto3.Session(aws_access_key_id=os.environ[f'aws_access_key_id{suffix}'],\
                        aws_secret_access_key=os.environ[f'aws_secret_access_key{suffix}'])
s3 = session


BUCKET_NAME = f'{os.environ[f"BUCKET_NAME{suffix}"]}'

def get_crypto_data(crypto_name):
  data_crypto = dict()
  url = f"https://api.coingecko.com/api/v3/coins/{crypto_name}?localization=false&tickers=false&market_data=false&sparkline=false"
  response = requests.get(url)
  data = response.json()
  data_crypto['name'] = data.get("name", "")
  data_crypto['symbol'] = data.get("symbol", "")
  data_crypto['genesis_date'] = data.get("genesis_date", "")
  data_crypto['sentiment_votes_up_percentage'] = str(data.get(
    "sentiment_votes_up_percentage", ""))
  data_crypto['sentiment_votes_down_percentage'] = str(data.get(
    "sentiment_votes_down_percentage", ""))
  data_crypto['market_cap_rank'] = str(data.get("market_cap_rank", ""))
  return data_crypto


def get_crypto_price(crypto_name, currency, start_dt=None, end_dt=None):
  url = f"https://api.coingecko.com/api/v3/coins/{crypto_name}/market_chart/range?vs_currency={currency}&from=1690257149&to=1692935549&precision=2"
  response = requests.get(url)
  return response.json()

def gen_intervales_date_day(start_dt, end_dt, interval):
  start_dt = pd.to_datetime(start_dt)
  end_dt = pd.to_datetime(end_dt)
  interval = pd.to_timedelta(interval)
  date_list = pd.date_range(start_dt, end_dt, freq=interval)
  return date_list

# response = get_crypto_price("bitcoin", "usd")
def convert_dt_to_timestamp(dt):
  dt = pd.to_datetime(dt)
  return dt.timestamp()

CRYPTO_TO_INGEST = "polkadot"

intervals = gen_intervales_date_day("2023-07-01", "2023-07-31", "5d")
pprint(intervals)

counter = 0
info_crypto = get_crypto_data(CRYPTO_TO_INGEST)
print(info_crypto)
for start_dt, end_dt in zip(intervals[:-1], intervals[1:]):
  #print(start_dt, end_dt)
  out_data = []
  response = get_crypto_price(CRYPTO_TO_INGEST, "usd", start_dt, end_dt)

  ds_row = dict()
  metric_dict = dict()
  for metric, values in response.items():
    for value in values:
      #print(value)
      if value[0] not in ds_row.keys():
        ds_row[value[0]] = {"timestamp": value[0], metric: str(value[1])}
      else:
        ds_row[value[0]].update({metric: str(value[1])})

  for ts, v in ds_row.items():
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
  print()
  wr.s3.to_parquet(df,
                   path=f"s3://{BUCKET_NAME}/cryptos_prices/",
                   dataset=True,
                   mode="overwrite_partitions",
                   partition_cols=["name", "year", "month", "day", "hour"],
                   boto3_session=session)
  counter += 1
  break

#pd.DataFrame(out_data).to_csv("bitcoin_price.csv")
#print(pd.DataFrame(out_data))

print(counter)

# print(get_crypto_data("bitcoin"))

print(convert_dt_to_timestamp("2023-01-01"))
print(convert_dt_to_timestamp("2023-01-02"))
