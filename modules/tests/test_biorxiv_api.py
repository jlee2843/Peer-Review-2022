'''
Standard Unit Testing in Python

import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
'''

from modules.utils.biorxiv_api import process_data

if __name__ == '__main__':
    # process_data(json_info: dict, section: str, keys: List[str], cursor: int)
    json_info = {}
    section = ''
    keys = []
    cursor = 0
    process_data(json_info, section, keys, cursor)
