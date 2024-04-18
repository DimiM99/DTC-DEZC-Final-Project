import pandas as pd
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(data, data_2, *args, **kwargs):
    dumps = data_2
    scraped = data

    # reformat the date format to match the one from dumps (cleaner)
    scraped['date'] = pd.to_datetime(scraped['date'])
    scraped['date'] = scraped['date'].dt.strftime('%Y-%m-%d %H:%M')

    # rename created and score columns into date and votes to match the scraped (also cleaner)
    dumps['date'] = dumps['created']
    dumps['votes'] = dumps['score']
    dumps.drop('created', axis=1, inplace=True)
    dumps.drop('score', axis=1, inplace=True)

    df_final = pd.concat([dumps, scraped])

    # add a column for the month and day (for partitioning purposes)
    df_final['MM_DD'] = df_final['date'].str[:7]

    return df_final
