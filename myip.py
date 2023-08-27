# get my ip public 
import requests

def get_my_ip_public():
  url = "https://api.ipify.org"
  response = requests.get(url)
  assert response.status_code == 200, f"Error in status code {response.status_code}"
  return response.text

print(get_my_ip_public())