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
    A base class for querying web data and making HTTP requests.

    Attributes:
        _url (str): The URL of the web resource.
        _keys (Tuple[str]): A tuple of keys used in the query.
        _col_names (List[str]): A list of column names for the result.
        _result (Any): The result of the query.
        _lock (Lock): A lock for thread safety.

    Properties:
        result (Any): The result of the query. This property is thread-safe.
        keys (Tuple[str]): The keys used in the query. This property is thread-safe.
        url (str): The URL of the web resource. This property is thread-safe.
        col_names (List[str]): The column names for the result. This property is thread-safe.

    Methods:
        execute(*args, **kwargs): Execute the query. This method is abstract and should be implemented by subclasses.

    Static Methods:
        retrieve_web_data(url: str, attempts: int = 0, attribute: str = "text") -> Any: Retrieve web data from the
                                                                                        specified URL.
        _get_response_content(response: Response, attribute: str) -> Any: Get the content of the HTTP response based on
                                                                          the specified attribute.
        make_request(attempts: int, url: str) -> Response: Make an HTTP request to the specified URL.

    Exceptions:
        ValueError: Raised when an invalid attribute is specified.
        HTTPError: Raised when the maximum number of attempts is reached and an HTTP error occurs during the request.
    """
    _url: str
    _keys: Tuple[str]
    _col_names: List[str]
    _result: Any = field(default=None)
    _lock: rwlock.RWLockFair = field(default_factory=rwlock.RWLockFair)

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

    def __init__(self, url: str, keys: Tuple[str], col_names: List[str], *args, **kwargs):
        self._rlock = self._lock.gen_rlock()
        self._wlock = self._lock.gen_wlock()
        with self._wlock:
            self._url = url
            self._keys = keys
            self._col_names = col_names

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
        _, json_info = self.fetch_json_data()
        json_info = json_info.result
        return json_info["messages"][0]["total"]

    def fetch_json_data(self, attempts: int = 0) -> Tuple[int, Query]:
        json_data = self.retrieve_web_data(self.url, attempts=attempts, attribute="json")
        with self._wlock:
            self._result = json_data
        return self.page_number, self

    def execute(self, attempts: int = 0) -> Tuple[int, Query]:
        return self.fetch_json_data(attempts)
