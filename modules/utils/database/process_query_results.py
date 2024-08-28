from datetime import datetime
from typing import Any, Dict, List

import doi
import numpy as np
import pandas as pd
from pandas import Series


def check_doi(x: str):
    """
    Check DOI validity.

    :param x: The input string representing a DOI (Digital Object Identifier).
    :return: The input DOI string if it is valid.
    :raises ValueError: If the input DOI is invalid.
    """
    if doi.validate_doi(x.strip()) is None:
        raise ValueError(f'invalid doi: {x.strip()}')
    else:
        return x.strip()


def get_value(data: Dict[str, Any], key: str) -> Any:
    """
    :param data: A dictionary containing key-value pairs.
    :param key: A string representing the key to be used to retrieve the value from the dictionary.
    :return: The value associated with the specified key in the dictionary.

    This function takes a dictionary and a key as input parameters. It attempts to retrieve the value associated with
    the key from the dictionary. If the key is found in the dictionary, the corresponding value is returned. If the
    key is not found, an exception is raised.

    Example usage:
    ```
    data = {'a': 1, 'b': 2, 'c': 3}
    key = 'b'
    result = get_value(data, key)
    print(result)  # Output: 2
    ```
    """
    result = None

    try:
        result = data[key]
    except Exception as e:
        print(f'key: {key} data: {data}\n{e}')
        raise e

    finally:
        return result


def convert_date(value: str):
    """
    :param value: A string representing a date in the format 'YYYY-MM-DD' with optional time information in the
                  form 'HH:MM:SS'.
    :return: A datetime object representing the date extracted from the given value. If the conversion fails,
             returns `pd.NaT`.
    """

    try:
        return datetime.strptime(value.strip().split(':')[0], '%Y-%m-%d')
    except Exception as e:
        print(e)
        return pd.NaT


def freq_count(x: pd.DataFrame, y: str) -> Series:
    """
    Perform frequency count on a pandas DataFrame series.

    :param x: a pandas DataFrame series.
    :param y: the column name of the series to perform frequency count on.
    :return: the frequency count of the given series.

    Example:
        >>> import pandas as pd
        >>> data = {'col1': [1, 2, 3, 2, 1, 3, 1]}
        >>> df = pd.DataFrame(data)
        >>> freq_count(df['col1'], 'col1')
        1    3
        2    2
        3    2
        Name: col1, dtype: int64
    """
    return x[y].value_counts()


def flatten(y: list) -> list:
    """
    Flattens a nested list and sorts it based on the first element of each sublist.

    :param y: a nested list
    :return: a flattened list sorted based on the first element of each sublist
    """
    return sorted([sublist for inner in y for sublist in inner], key=lambda x: x[0])


# flatten = lambda y: sorted([sublist for inner in y for sublist in inner],
#                           key=lambda x: x[0])

def create_df(x: np.ndarray, y: List[str]) -> pd.DataFrame:
    """
    Create a pandas DataFrame from the given numpy array and list of column names.

    :param x: A numpy array containing the data.
    :type x: np.ndarray
    :param y: A list of column names.
    :type y: List[str]
    :return: A pandas DataFrame created from the input data with column names and an index.
    :rtype: pd.DataFrame
    """
    return pd.DataFrame(data=x[:, 1:], index=x[:, 0], columns=y)


# create_df = lambda x, y: pd.DataFrame(data=x[:, 1:], index=x[:, 0], columns=y)

if __name__ == '__main__':
    # get_web_data(counter=0, url='hello', attr='.')
    pass
