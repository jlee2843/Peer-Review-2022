import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import RLock
from typing import Tuple, List, Any

import requests
from requests import HTTPError, Response


@dataclass
class Query(ABC):
    """
    This class represents a Query object for executing database queries.

    Attributes:
        _url (str): The URL of the database.
        _keys (Tuple[str]): The keys required for authentication.
        _col_names (List[str]): The names of the columns in the result set.
        _result: The result of the executed query.

    Methods:
        result (property): Getter for the result attribute.
        keys (property): Getter for the keys attribute.
        url (property): Getter for the url attribute.
        col_names (property): Getter for the col_names attribute.
        cursor (property): Getter for the cursor attribute.
        execute (abstractmethod): Executes the query.

    Example:
        url = 'https://example.com'
        keys = ('key1', 'key2')
        col_names = ['column1', 'column2', 'column3']
        query = Query(url, keys, col_names)
        query.execute()
        result = query.result
    """

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str]) -> None:
        self._url: str = url
        self._keys: Tuple[str] = keys
        self._col_names: List[str] = col_names
        self._result: Any = None
        self._Lock: RLock = RLock()

    @property
    def result(self) -> Any:
        return self._result

    @result.setter
    def result(self, result: Any):
        _result = result

    @property
    def keys(self):
        return self._keys

    @property
    def url(self) -> str:
        return self._url

    @property
    def col_names(self) -> List[str]:
        return self._col_names

    @abstractmethod
    def execute(self, attr: str = 'json'):
        pass

    def get_json_data(self, counter: int, cursor: int) -> tuple[int, Any]:
        """
        Retrieve JSON data from a web service.

        :param counter: The counter value to be passed to the web service.
        :param cursor: The cursor value to be passed to the web service.
        :return: A tuple containing the updated cursor value and the query object with the JSON data set as the result.
        """

        self.result = self.get_web_data(counter, self.url, "json")
        return cursor, self

    @staticmethod
    def get_web_data(counter: int, url: str, attr: str = "text") -> Any:
        """
        Retrieves web data from the given URL based on the specified attribute.

        :param counter: The number of connection attempts to make.
        :param url: The URL from which to retrieve the web data.
        :param attr: The attribute to retrieve from the web data. Default is "text".
                     Valid values are "text", "content", and "json".
        :return: The retrieved web data in text format (default), json format, or in byte format.
        :raises ValueError: If the specified attribute is not valid (not 'text', 'content', or 'json').
        """

        result: Any = None
        valid = ['text', 'content', 'json']
        attr = attr.strip().lower()

        if attr not in valid:
            raise ValueError(f'get_web_data: {attr} is an unexpected attr ({valid}')

        try:
            if attr == 'json':
                result = getattr(Query.connect_url(counter, url), attr)()
            else:
                result = getattr(Query.connect_url(counter, url), attr)

        except TypeError:
            pass
        finally:
            return result

    @staticmethod
    def connect_url(counter: int, url: str) -> Response:
        """

        Connect to the specified URL.

        :param counter: The number of times the connection has been attempted.
        :type counter: int

        :param url: The URL to connect to.
        :type url: str

        :return: The HTTP response from the URL.
        :rtype: Response

        """

        response: Response = Response()

        try:
            response = requests.get(url)
        except Exception as e:
            if counter == 10:
                raise HTTPError from e

            time.sleep(300)
            return Query.connect_url(counter + 1, url)

        finally:
            response.raise_for_status()
            return response


# noinspection PyUnresolvedReferences

@dataclass
class BioRvixQuery(Query):
    """
    This class represents a query to fetch data from the BioRvix database.

    It extends the base class Query.

    Attributes:
        cursor (int): The database cursor used to execute the query.
        url (str): The URL of the BioRvix database.
        result (Any): The result of the query execution.

    Methods:
        execute(attr: str = 'json') -> Any:
            Executes the query and returns the result.

    """

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str], cursor: int = 0) -> None:
        with self._lock:
            super().__init__(url, keys, col_names)
            self._cursor = cursor

    @property
    def cursor(self) -> int:
        return self._cursor

    def execute(self, attr: str = 'json') -> Any:
        with self._lock:
            self.result = Query.get_web_data(self.cursor, self.url, attr)
        return self.result
