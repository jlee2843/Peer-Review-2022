import time
from typing import Union, Tuple, Any

import doi
import requests
from requests import HTTPError, Response


def get_json_data(counter: int, cursor: int, url: str) -> Tuple[int, Any]:
    return cursor, get_web_data(counter, url, "json")


def get_web_data(counter: int, url: str, attr: str = "text") -> Union[str, bytes]:
    return getattr(connect_url(counter, url), attr)


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
        raise Exception(f'invalid doi: {x.strip()}')
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
