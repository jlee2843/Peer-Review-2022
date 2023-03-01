from typing import Union, List, Iterator
import time

def get_json_data (counter:int, cursor:int,  url: str, attr: str =
        "text") -> List:

    import requests
    import json

    try:
        request_API = requests.get(url)
        #print(f"{url} request_API: {request_API}")
        return cursor, json.loads(getattr(request_API, attr))
    except Exception as e:
        if counter == 10 :
            raise e
            
        time.sleep(300)
        return cursor, get_json_data(counter + 1, cursor, url)

def get_web_data (counter:int, cursor:int,  url: str, attr: str =
        "text") -> List:

    import requests
    import json

    try:
        request_API = requests.get(url)
        #print(f"{url} request_API: {request_API}")
        return cursor, json.loads(getattr(request_API, attr))
    except Exception as e:
        if counter == 10 :
            raise e
            
        time.sleep(300)
        return cursor, get_web_data(counter + 1, url)

def checkDOI(x:str):
    import doi
    if doi.validate_doi(x.strip()) is None:
        raise Exception(f'invalid doi: {x.strip()}')
    else:
        return x.strip()

def process_data(json_info, section:str , keys:List[str], cursor:int, disable:bool = True) -> List:
    journal_list = [[entry + cursor] + [getValue(journal, key) for key in keys] for entry, journal in enumerate(json_info[section])]
    if disable is False:
        time.sleep(60)

    return journal_list

def getValue(data, key):
    result = None

    try:
        result = data[key]
    except Exception as e:
        print (f'key: {key} data: {data}\n{e}')
        raise e

    finally:
        return result

