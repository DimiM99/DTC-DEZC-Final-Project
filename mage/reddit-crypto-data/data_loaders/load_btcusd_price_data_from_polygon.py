import time
import os
from datetime import datetime

import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    end_date = time.time() - 60 * 60 * 24
    start_date = end_date - 60 * 60 * 24 * 360 * 2
    end_date = datetime.fromtimestamp(end_date).strftime('%Y-%m-%d')
    start_date = datetime.fromtimestamp(start_date).strftime('%Y-%m-%d')
    api_key = os.environ['POLYGON_API_KEY']

    url = f'https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}'
    response = requests.get(url)
    data = response.json()['results']
    result = pd.DataFrame(data)
    result['date'] = pd.date_range(start_date, periods=len(result), freq='D')
    result['date'] = result['date'].dt.date
    return result

