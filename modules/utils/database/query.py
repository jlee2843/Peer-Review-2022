from typing import Tuple, List


class Query:
    """
    Class Query

    This class represents a query object used to interact with a database.

    Attributes:
        _result (object): The result of the query

    Methods:
        __init__(self, url: str, keys: Tuple[str], col_names: List[str]) -> object:
            Initializes a new Query object with the given URL, keys, and column names.

        get_result(self):
            Returns the result of the query

        get_keys(self):
            Returns the keys used in the query

        get_url(self):
            Returns the URL used in the query

        get_col_names(self):
            Returns the column names used in the query

        set_result(self, data):
            Sets the result of the query to the given data
    """
    _result = None

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str]) -> None:
        self._url = url
        self._keys = keys
        self._col_names = col_names

    def get_result(self):
        return self._result

    def get_keys(self):
        return self._keys

    def get_url(self):
        return self._url

    def get_col_names(self):
        return self._col_names

    def set_result(self, data):
        self._result = data
