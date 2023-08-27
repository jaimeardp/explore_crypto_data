import requests
from utils.wrap_datetimes import convert_dt_to_timestamp

_ENDPOINT_API = "https://api.coingecko.com/api/v3/"

def get_crypto_data(crypto_name):
  data_crypto = {}
  url = f"{_ENDPOINT_API}/coins/{crypto_name}?localization=false&tickers=false&market_data=false&sparkline=false"
  response = requests.get(url)
  assert response.status_code == 200, f"Error in status code {response.status_code}"
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

def get_crypto_price_historical(crypto_name, currency, start_dt=None, end_dt=None):
  assert start_dt is not None and end_dt is not None, "start_dt and end_dt is None"
  start_dt = convert_dt_to_timestamp(start_dt)
  end_dt = convert_dt_to_timestamp(end_dt)
  #print(f"start_dt: {start_dt}, end_dt: {end_dt}")
  #url = f"{_ENDPOINT_API}/coins/{crypto_name}/market_chart/range?vs_currency={currency}&from=1690257149&to=1692935549&precision=2"
  url = f"{_ENDPOINT_API}/coins/{crypto_name}/market_chart/range?vs_currency={currency}&from={start_dt}&to={end_dt}&precision=2"
  response = requests.get(url)
  #print(response)
  assert response.status_code == 200, f"Error in status code {response.status_code}"
  return response.json()
