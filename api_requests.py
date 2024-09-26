# api_requests.py

import requests


# call currency exchange API
def fetch_currency_data(token, start_date, end_date):
    url = f"https://api.currencybeacon.com/v1/timeseries?api_key={token}&start_date={start_date}&end_date={end_date}&symbols=AUD"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
