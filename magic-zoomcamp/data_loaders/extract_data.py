import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # Write download function
    def fetch_data(service: str, year: int, month: int):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year:04d}-{month:02d}.parquet'
        return pd.read_parquet(url)
    
    # Get arguments
    service = kwargs.get('taxi_service')
    year    = int(kwargs.get('year'))
    month   = int(kwargs.get('month'))
    
    # Return pandas.DataFrame
    return fetch_data(service, year, month)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
