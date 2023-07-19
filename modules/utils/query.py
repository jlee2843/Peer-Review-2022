import time
from datetime import datetime
from typing import Tuple, Any, List

import doi
import numpy as np
import pandas as pd
import requests
from requests import HTTPError, Response


class Query:
    _result = None

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str]):
        self._url = url
        self._keys = keys
        self._col_names = col_names

    def get_result(self):
        return self._result

    def get_keys(self):
        return self._keys

    def get_url(self):
        return self._url

    def get_col_names(self):
        return self._col_names

    def set_result(self, data):
        self._result = data


def get_json_data(counter: int, cursor: int, query: Query) -> Tuple[int, Query]:
    query.set_result(get_web_data(counter, query.get_url(), "json"))
    return cursor, query


def get_web_data(counter: int, url: str, attr: str = "text") -> Any:
    """

    :param counter:
    :param url:
    :param attr:
    :return:
    """
    result: Any = None
    valid = ['text', 'content', 'json']
    attr = attr.strip().lower()

    if attr not in valid:
        print(f'attr: {attr}')
        raise ValueError(f'get_web_data: {attr} is an unexpected attr ({valid}')

    try:
        if attr == 'json':
            result = getattr(connect_url(counter, url), attr)()
        else:
            result = getattr(connect_url(counter, url), attr)

    except TypeError:
        pass
    finally:
        return result


def connect_url(counter: int, url: str) -> Response:
    response: Response = Response()

    try:
        response = requests.get(url)
    except Exception as e:
        if counter == 10:
            raise HTTPError from e

        time.sleep(300)
        return connect_url(counter + 1, url)

    finally:
        response.raise_for_status()
        return response


def check_doi(x: str):
    if doi.validate_doi(x.strip()) is None:
        raise ValueError(f'invalid doi: {x.strip()}')
    else:
        return x.strip()


def get_value(data: dict, key: str):
    result = None

    try:
        result = data[key]
    except Exception as e:
        print(f'key: {key} data: {data}\n{e}')
        raise e

    finally:
        return result


def convert_date(value: str):
    try:
        return datetime.strptime(value.strip().split(':')[0], '%Y-%m-%d')
    except Exception as e:
        print(e)
        return pd.NaT


def freq_count(x, y):
    return x[y].value_counts()


def flatten(y):
    return sorted([sublist for inner in y for sublist in inner], key=lambda x: x[0])


# flatten = lambda y: sorted([sublist for inner in y for sublist in inner],
#                           key=lambda x: x[0])

def create_df(x: np.ndarray, y: List):
    return pd.DataFrame(data=x[:, 1:], index=x[:, 0], columns=y)


# create_df = lambda x, y: pd.DataFrame(data=x[:, 1:], index=x[:, 0], columns=y)

if __name__ == '__main__':
    get_web_data(counter=0, url='hello', attr='.')
