import pandas as pd

def gen_intervales_date_day(start_dt, end_dt, interval):
  start_dt = pd.to_datetime(start_dt)
  end_dt = pd.to_datetime(end_dt)
  interval = pd.to_timedelta(interval)
  date_list = pd.date_range(start_dt, end_dt, freq=interval)
  return date_list

def convert_dt_to_timestamp(dt):
  dt = pd.to_datetime(dt)
  return dt.timestamp()