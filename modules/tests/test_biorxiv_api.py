'''
Standard Unit Testing in Python

import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
'''

from modules.utils.query import Query
import pytest


@pytest.fixture()
def query():
    return Query("test")


def test_query(query):
    assert isinstance(query, Query)


'''
if __name__ == '__main__':
    # process_data(json_info: dict, section: str, keys: List[str], cursor: int)
    json_info = {}
    section = ''
    keys = []
    cursor = 0
    process_data(json_info, section, keys, cursor)
'''
