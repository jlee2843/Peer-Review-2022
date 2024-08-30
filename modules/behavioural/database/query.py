import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Tuple, List, Any, Set

import requests
from readerwriterlock import rwlock
from requests import HTTPError, Response, RequestException

MAX_ATTEMPTS: int = 10
SLEEP_INTERVAL: int = 300
VALID_ATTRIBUTES: Set[str] = {'text', 'content', 'json'}


@dataclass
class Query(ABC):
    """
    The `Query` class is an abstract base class that provides a common interface for querying data from a web source.

    Attributes:
        _lock (rwlock.RWLockFair): A fair reader-writer lock used to synchronize access to the class attributes.
        _rlock (rwlock.RLock): A reader lock acquired from `_lock` for read-only operations.
        _wlock (rwlock.RLock): A reader lock acquired from `_lock` for write operations.
        _url (str): The URL of the web source to query.
        _keys (Tuple[str]): The keys to use in the query.
        _col_names (List[str]): The names of the columns in the query result.
        _result (Any): The result of the query.

    Methods:
        __init__(self, url: str, keys: Tuple[str], col_names: List[str], result: Any = None, *args, **kwargs)
            Initializes a new instance of the `Query` class.

            Parameters:
                url (str): The URL of the web source to query.
                keys (Tuple[str]): The keys to use in the query.
                col_names (List[str]): The names of the columns in the query result.
                result (Any, optional): The initial result of the query. Defaults to None.

        execute(self, *args, **kwargs)
            Abstract method that needs to be implemented by concrete subclasses.
            Executes the query.

        retrieve_web_data(url: str, attempts: int = 0, attribute: str = "text") -> Any
            Static method that retrieves data from a web source.

            Parameters:
                url (str): The URL of the web source to retrieve data from.
                attempts (int, optional): The number of attempts made to retrieve the data. Defaults to 0.
                attribute (str, optional): The attribute of the response to retrieve. Defaults to "text".

            Returns:
                Any: The retrieved data.

        _get_response_content(response: Response, attribute: str) -> Any
            Static method that extracts the content from a response object.

            Parameters:
                response (Response): The response object.
                attribute (str): The attribute of the response to extract the content from.

            Returns:
                Any: The extracted content.

        _make_request(attempts: int, url: str) -> Response
            Static method that makes a HTTP GET request to a URL.

            Parameters:
                attempts (int): The number of attempts made to make the request.
                url (str): The URL to make the request to.

            Returns:
                Response: The response object.
    """

    @property
    def result(self) -> Any:
        with self._rlock:
            return self._result

    @property
    def keys(self) -> Tuple[str]:
        with self._rlock:
            return self._keys

    @property
    def url(self) -> str:
        with self._rlock:
            return self._url

    @property
    def col_names(self) -> List[str]:
        with self._rlock:
            return self._col_names

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str], result: Any = None, *args, **kwargs):
        import re
        self._lock = rwlock.RWLockFair()
        self._rlock = self._lock.gen_rlock()
        self._wlock = self._lock.gen_wlock()
        self._url = url
        self._keys = keys
        self._col_names = col_names
        self._result = result

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @staticmethod
    def retrieve_web_data(url: str, attempts: int = 0, attribute: str = "text") -> Any:
        attribute = attribute.strip().lower()
        if attribute not in VALID_ATTRIBUTES:
            raise ValueError(f'Invalid attribute: {attribute}. Expected one of {VALID_ATTRIBUTES}')

        response = Query._make_request(attempts, url)
        return Query._get_response_content(response, attribute)

    @staticmethod
    def _get_response_content(response: Response, attribute: str) -> Any:
        try:
            response_func = getattr(response, attribute)
            return response_func() if attribute == 'json' else response_func
        except TypeError:
            return None

    @staticmethod
    def _make_request(attempts: int, url: str) -> Response:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except RequestException as e:
            if attempts >= MAX_ATTEMPTS:
                raise HTTPError from e
            wait_time = SLEEP_INTERVAL * (2 ** attempts)  # Exponential backoff
            time.sleep(wait_time)
            return Query._make_request(attempts + 1, url)


@dataclass
class BioRvixQuery(Query):
    """
    A class representing a query for BioRvix documents.

    Args:
        url (str): The URL to query.
        keys (Tuple[str]): The keys to use for the query.
        col_names (List[str]): The list of column names.
        page (int, optional): The page number to start the query from. Defaults to 0.

    Attributes:
        _page (int): The current page number.

    """
    _page: int = 0

    @property
    def page_number(self) -> int:
        with self._rlock:
            return self._page

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str], page: int = 0):
        super().__init__(url, keys, col_names, None)
        with self._wlock:
            self._page = page

    def get_total_entries(self) -> int:
        import re
        end_point: str = self.url
        m = re.search(r'\d+$', end_point)
        # if the string ends in digits m will be a Match object, or None otherwise.
        if m is None:
            end_point = end_point + '0'
        json_info = self.retrieve_web_data(end_point, attribute='json')
        return int(json_info["messages"][0]["total"])

    def fetch_json_data(self, attempts: int = 0) -> Tuple[int, Query]:
        json_data = self.retrieve_web_data(self.url, attempts=attempts, attribute="json")
        with self._wlock:
            self._result = json_data
        return self.page_number, self

    def execute(self, attempts: int = 0) -> Tuple[int, Query]:
        return self.fetch_json_data(attempts)
