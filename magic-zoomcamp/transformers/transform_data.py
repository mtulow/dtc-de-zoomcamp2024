import re
import pandas as pd


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    
    # Create a function to convert camel case string to snake case 
    def camel_to_snake(text: str) -> str:
        """
        Converts a camel case string to snake case.
        Example: camelToSnakeCase -> camel_to_snake_case
        """
        # Use regex to identify word boundaries and replace them with underscores
        # followed by the lowercase version of the matched word.
        # Additionally, handle consecutive uppercase letters by appending them as lowercase.
        return re.sub(r'(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', '_', text).lower()
    
    # Create column dictionary for new names
    col_mapper = {col: camel_to_snake(col)for col in data.columns}
    col_mapper['lpep_pickup_datetime'] = 'pickup_datetime'
    col_mapper['lpep_dropoff_datetime'] = 'dropoff_datetime'

    
    # Rename columns
    data.rename(columns=col_mapper, inplace=True)

    data.insert(
        list(data.columns).index('pickup_datetime'),
        'pickup_date',
        data['pickup_datetime'].dt.date,
    )
    data.insert(
        list(data.columns).index('dropoff_datetime'),
        'dropoff_date',
        data['dropoff_datetime'].dt.date,
    )    

    data['pickup_datetime'] = data['pickup_datetime'].apply(str)
    data['pickup_date'] = data['pickup_date'].apply(str)
    data['dropoff_datetime'] = data['dropoff_datetime'].apply(str)
    data['dropoff_date'] = data['dropoff_date'].apply(str)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
