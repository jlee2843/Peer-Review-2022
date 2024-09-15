import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

import doi
import numpy as np
import pandas as pd
from pandas import Series


def validate_doi(doi_string: str) -> str:
    """
    Validates and returns the given DOI string if it is valid.
    :param doi_string: The input string representing a DOI.
    :return: The validated DOI string.
    :raises ValueError: If the input DOI is invalid.
    """
    doi_string = doi_string.strip()
    if not doi.validate_doi(doi_string):
        raise ValueError(f'Invalid DOI: {doi_string}')
    return doi_string


def get_dict_value(data: Dict[str, Any], key: str) -> Any:
    """
    Retrieve the value associated with the specified key from the dictionary.
    :param data: A dictionary containing key-value pairs.
    :param key: The key whose value needs to be retrieved.
    :return: The value associated with the specified key.
    :raises KeyError: If the key is not found in the dictionary.
    """
    try:
        return data[key]
    except KeyError as e:
        print(f'Key: {key}, Data: {data}\n{e}')
        raise


def parse_date(date_string: str) -> Union[datetime, pd.NaT]:
    """
    Parses and returns a datetime object from the given date string.
    :param date_string: A string representing a date in 'YYYY-MM-DD' format.
    :return: A datetime object for the parsed date.
    :returns: pd.NaT if the parsing fails.
    """
    try:
        return datetime.strptime(date_string.strip().split(':')[0], '%Y-%m-%d')
    except Exception as e:
        print(e)
        return pd.NaT


def calculate_frequency(df: pd.DataFrame, column: str) -> Series:
    """
    Performs frequency count on a specified column of a DataFrame.
    :param df: A pandas DataFrame.
    :param column: The column name to perform frequency count on.
    :return: Series representing the frequency count of the specified column.
    """
    return df[column].value_counts()


def flatten_list(nested_list: List[List[Any]]) -> List[Any]:
    """
    Flattens and sorts a nested list based on the first element of each sublist.
    :param nested_list: A nested list of elements.
    :return: A flattened and sorted list based on the first element of each sublist.
    """
    return sorted([sublist for inner in nested_list for sublist in inner], key=lambda x: x[0])


def create_dataframe(data: np.ndarray, columns: List[str]) -> pd.DataFrame:
    """
    Creates a DataFrame from numpy array and list of column names.
    :param data: A numpy array containing data.
    :param columns: A list of column names.
    :return: A pandas DataFrame created from the input data.
    """
    return pd.DataFrame(data=data[:, 1:], index=data[:, 0], columns=columns)


def process_json_data(json_data: dict, section: str, keys: Tuple[str], offset: int, add_delay: bool = False) -> List[
    List[Any]]:
    """
    Processes and returns data from a specified section of the JSON data.
    :param json_data: A dictionary containing JSON data.
    :param section: The section of JSON data to process.
    :param keys: The keys to extract values from each journal entry.
    :param offset: Value to increment each journal entry index by.
    :param add_delay: Whether to add a delay before processing.
    :return: List of processed journal entry data.
    """
    if add_delay:
        time.sleep(60)

    def process_entry(entry_index: int, journal_entry: Dict[str, Any]) -> List[Any]:
        return [entry_index + offset] + [get_dict_value(journal_entry, key) for key in keys]

    return [process_entry(index, entry) for index, entry in enumerate(json_data[section])]


if __name__ == '__main__':
    pass
