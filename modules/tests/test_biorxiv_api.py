'''
Standard Unit Testing in Python

import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
'''

import pytest

from modules.utils.query import Query, get_web_data, get_json_data


@pytest.fixture()
def prepub_query():
    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    url = 'https://api.biorxiv.org/details/biorxiv/10.1101/339747'
    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')
    col_names = ["DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Date", "Version", "Type",
                 "Category", "Xml", "Published"]
    query = Query(url, keys, col_names)
    result = get_json_data(0, 0, query)
    return result[1]


@pytest.fixture()
def pub_query():
    url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    keys = ("preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
            "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
            "published_date")
    col_names = ["DOI", "pub_DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Category", "Journal",
                 "Preprint_Date", "Published_Date"]
    query = Query(url, keys, col_names)
    return get_json_data(0, 0, query)[1]


@pytest.fixture()
def query():
    url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    keys = ("preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
            "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
            "published_date")
    col_names = ["DOI", "pub_DOI", "Title", "Authors", "Corresponding_Authors", "Institution", "Category", "Journal",
                 "Preprint_Date", "Published_Date"]
    query = Query(url, keys, col_names)
    return query


def test_query(query):
    assert isinstance(query, Query)


def test_get_keys(query):
    keys = ("preprint_doi", "published_doi", "preprint_title", "preprint_authors", "preprint_author_corresponding",
            "preprint_author_corresponding_institution", "preprint_category", "published_journal", "preprint_date",
            "published_date")
    assert query.get_keys() is not None
    assert query.get_keys() is keys


def test_get_query_result(pub_query, query):
    query.set_result('test')
    assert query.get_result() is not None
    assert query.get_result() is 'test'
    assert query.get_url() is 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    query.set_result(get_web_data(0, query.get_url(), 'json'))
    assert query is not None
    _, result = get_json_data(0, 0, query)
    assert get_json_data(0, 0, query) is not None
    assert result.get_result() is not None
    assert pub_query.get_result() is not None
    assert pub_query.get_result() == query.get_result()


def test_get_web_data():
    # url = 'https://api.biorxiv.org/pubs/medrxiv/10.1101/2021.04.29.21256344'
    url = 'https://api.biorxiv.org/details/biorxiv/10.1101/339747'
    result = get_web_data(0, url, 'json')
    assert isinstance(result, dict)
    result = get_web_data(0, url)
    test = '123'
    assert isinstance(result, str)
    result = get_web_data(0, url, 'content')
    assert isinstance(result, bytes)


def test_invalid_attr():
    with pytest.raises(ValueError):
        get_web_data(0, 'hello', 'texTs')
        get_web_data(0, 'hello', '')
        get_web_data(0, 'hello', 'text.')


def test_valid_attr(query):
    try:
        get_web_data(0, query.get_url(), 'TEXT')
        get_web_data(0, query.get_url(), 'text ')
        get_web_data(0, query.get_url(), ' text')
        get_web_data(0, query.get_url(), ' text ')
    except ValueError as exc:
        assert False, f'get_web_data: {exc}'


def test_process_biorxiv_query(prepub_query):
    from modules.utils.biorxiv_api import process_data

    result = process_data(prepub_query.get_result(), 'collection', prepub_query.get_keys(), 0)
    assert result[0][1] is not None


def test_get_value(prepub_query):
    keys: tuple = ('doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date',
                   'version', 'type', 'category', 'jatsxml', 'published')

    result: dict = prepub_query.get_result()['collection'][0]
    collection = list(result.keys())
    try:
        _ = [result[key] for key in collection]
    except KeyError as exc:
        assert False, f'raised an exception {exc}'
