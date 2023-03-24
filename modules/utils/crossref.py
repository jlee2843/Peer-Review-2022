import json
from typing import List, Dict

import crossref_commons.retrieval as xref


def get_publication_info(doi: str) -> json:
    result = xref.get_publication_as_json(doi)
    print(type(result))
    print(result)
    print(result['link'])
    return result


def process_publication_info(result: json) -> Dict:
    result = {}
    # create publication related objects
    # get publication list - xml
    # get publication list - pdf
    # get publication list - webcrawl
    return result


def process_link(links: List[Dict], key: str, value: str) -> List[str]:
    result: Dict = {}
    for link in links:
        tmp = link.get(key)
        if tmp is None:
            continue
        elif tmp == value:
            result[value] = link.get('url')
    pass


def test_link(url: str, attr: str = 'text'):
    from modules.utils.query import get_web_data
    result = None
    result = get_web_data(0, url, attr)
    pass


if __name__ == '__main__':
    # get_publication_info('10.5555/515151')
    # get_publication_info("10.1101/104778")
    get_publication_info("10.1038/s41598-017-04402-4")
    get_publication_info("10.1109/MM.2019.2910009")
