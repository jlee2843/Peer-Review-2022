import time
from typing import Union, List

import doi
import requests


def get_json_data(counter: int, cursor: int, url: str, attr: str = "text") -> List:
    import json

    return cursor, json.loads(get_web_data(counter, url))


def get_web_data(counter: int, url: str, attr: str = "text") -> Union[str, bytes]:
    try:
        return getattr(requests.get(url), attr)
    except Exception as e:
        if counter == 10:
            raise e

        time.sleep(300)
        return get_web_data(counter + 1, url)


def check_doi(x: str):
    if doi.validate_doi(x.strip()) is None:
        raise Exception(f'invalid doi: {x.strip()}')
    else:
        return x.strip()


def process_data(json_info, section: str, keys: List[str], cursor: int, disable: bool = True) -> List:
    journal_list = [[entry + cursor] + [get_value(journal, key) for key in keys] for entry, journal in
                    enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    return journal_list


def get_value(data, key):
    result = None

    try:
        result = data[key]
    except Exception as e:
        print(f'key: {key} data: {data}\n{e}')
        raise e

    finally:
        return result
