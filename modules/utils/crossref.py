import json

import crossref_commons.retrieval as xref


def get_publication_info(doi: str) -> json:
    result = xref.get_publication_as_json(doi)
    print(type(result))
    print(result)
    print(result['link'])
    return result


def process_publication_info(result: json):
    pass


if __name__ == '__main__':
    get_publication_info('10.5555/515151')
