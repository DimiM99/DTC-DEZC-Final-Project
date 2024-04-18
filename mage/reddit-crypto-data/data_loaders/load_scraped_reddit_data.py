import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_scraped_data_from_csv(*args, **kwargs):
    return pd.read_csv("/home/src/data/output/reddit_data.csv")

