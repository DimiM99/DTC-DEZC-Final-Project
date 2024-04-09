import time
import os
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    end_date = time.time()
    start_date = end_date - 30 * 24 * 60 * 60
    api_key = os.environ['POLYGON_API_KEY']

    url = f'https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}'
    response = requests.get(url)
    data = response.json()['results']
    result = pd.DataFrame(data)
    result['date'] = pd.date_range(start_date, periods=len(result), freq='D')
    result['date'] = result['date'].dt.date
    return result



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
