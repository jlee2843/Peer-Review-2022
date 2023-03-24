import time
from typing import Union, List, Tuple, Any

import doi
import requests
from requests import HTTPError


def get_json_data(counter: int, cursor: int, url: str) -> Tuple[int, Any]:
    return cursor, get_web_data(counter, url, "json")


def get_web_data(counter: int, url: str, attr: str = "text") -> Union[str, bytes]:
    from requests import Response

    response: Response

    try:
        response = requests.get(url)
    except HTTPError as e:
        raise e
    except Exception as e:
        if counter == 10:
            raise e

        time.sleep(300)
        return get_web_data(counter + 1, url)

    finally:
        response.raise_for_status()
        return getattr(response, attr)


def check_doi(x: str):
    if doi.validate_doi(x.strip()) is None:
        raise Exception(f'invalid doi: {x.strip()}')
    else:
        return x.strip()


def process_data(json_info: dict, section: str, keys: List[str], cursor: int, disable: bool = True) -> List:
    journal_list = [[entry + cursor] + [get_value(journal, key) for key in keys] for entry, journal in
                    enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    return journal_list


def get_value(data: dict, key: str):
    result = None

    try:
        result = data[key]
    except Exception as e:
        print(f'key: {key} data: {data}\n{e}')
        raise e

    finally:
        return result
