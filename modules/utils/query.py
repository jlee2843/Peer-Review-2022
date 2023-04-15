import time
from typing import Tuple, Any

import doi
import requests
from requests import HTTPError, Response


class Query:
    def __init__(self, data):
        self._result = data

    def get_result(self):
        return self._result


def get_json_data(counter: int, cursor: int, url: str) -> Tuple[int, Any]:
    return cursor, Query(get_web_data(counter, url, "json"))


def get_web_data(counter: int, url: str, attr: str = "text") -> Any:
    return getattr(connect_url(counter, url), attr)()


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


if __name__ == '__main__':
    url = 'https://api.biorxiv.org/details/biorxiv/2020-08-21/2020-08-28'
    print(Query(get_web_data(0, url, 'json')).get_result())
