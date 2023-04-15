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

from modules.utils.query import Query, get_web_data


@pytest.fixture()
def query():
    return Query("test")


@pytest.fixture()
def real_query():
    url = 'https://api.biorxiv.org/details/biorxiv/2020-08-21/2020-08-28'
    return Query(get_web_data(0, url, 'json'))


def test_query(query):
    assert isinstance(query, Query)


def test_get_query_result(query):
    assert query.get_result() is not None


def test_get_web_data():
    url = 'https://api.biorxiv.org/details/biorxiv/2020-08-21/2020-08-28'
    result = get_web_data(0, url, 'json')
    assert isinstance(result, dict)
    result: str = get_web_data(0, url)
    test = '123'
    assert isinstance(result, test)


def test_process_biorxiv_query(real_query, capsys):
    from modules.utils.biorxiv_api import process_data

    keys = ['doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'date', 'version',
            'type', 'category', 'toxml', 'published'],
    print(type(real_query.get_result()))
    capsys.readouterr().out
    assert isinstance(real_query.get_result(), dict)
    assert process_data(real_query.get_result(), 'collection', keys, 0) is not []
