import time
import requests
import pandas as pd
import datetime


def get_data():
    end_date = time.time()
    start_date = end_date - 30 * 24 * 60 * 60
    api_key = 'set me up'
    url = f'https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/{datetime.date.fromtimestamp(start_date)}/{datetime.date.fromtimestamp(end_date)}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}'
    response = requests.get(url)
    data = response.json()['results']
    result = pd.DataFrame(data)
    result['date'] = pd.date_range(start_date, periods=len(result), freq='D')
    result['date'] = result['date'].dt.date

    result.to_csv('../data-dumps/btcusd-price-data.csv', index=False)


get_data()
