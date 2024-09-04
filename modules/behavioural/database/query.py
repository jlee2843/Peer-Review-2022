import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, List, Any

import requests
from readerwriterlock import rwlock
from requests import HTTPError, Response, RequestException

MAX_ATTEMPTS: int = 10
SLEEP_INTERVAL: int = 300
VALID_ATTRIBUTES: Tuple[str, str, str] = ('text', 'content', 'json')


@dataclass
class Query(ABC):
    """
    Dataclass representing a Query that includes properties for
    query parameters and methods for executing and processing queries.
    It also includes static methods for retrieving web data.

    Attributes
    ----------
    MAX_ATTEMPTS : int
        The maximum number of attempts to retrieve web data.
    SLEEP_INTERVAL : int
        The interval (in seconds) to wait between attempts.
    VALID_ATTRIBUTES : Tuple[str]
        Allowed attributes for response content.

    Properties
    ----------
    result : Any
        The result of the query.
    keys : Tuple[str]
        The keys associated with the query.
    url : str
        The URL of the query.
    col_names : List[str]
        The column names for the query result.

    Methods
    -------
    __init__(url, keys, col_names, result, *args, **kwargs)
        Initializes a new Query.
    execute(*args, **kwargs)
        Abstract method to execute the query.
    retrieve_web_data(url, attempts, attribute)
        Retrieves web data with the specified number of attempts and attribute.
    _get_response_content(response, attribute)
        Gets the content of the response based on the specified attribute.
    _make_request(attempts, url)
        Makes a web request with exponential backoff for retrying.
    """
    MAX_ATTEMPTS: int = 10
    SLEEP_INTERVAL: int = 300
    VALID_ATTRIBUTES: Tuple[str] = ('text', 'content', 'json')

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
        BioRvixQuery class is a specialized type of Query for handling specific operations for a BioRvix dataset.

        _page : int
            Tracks the current page of data being accessed.

        Methods
        -------
        page_number() -> int
            Returns the current page number in a thread-safe manner.

        __init__(url: str, keys: Tuple[str], col_names: List[str], page: int = 0)
            Initializes the BioRvixQuery with specified parameters and sets the page number.

        get_total_entries() -> int
            Retrieves the total number of entries available from the remote source.

        fetch_json_data(attempts: int = 0) -> Tuple[int, Query]
            Fetches JSON data from the specified URL, updates the internal result, and returns the page number along with the Query object.

        execute(attempts: int = 0) -> Tuple[int, Query]
            Wrapper around fetch_json_data to initiate the data fetching process.
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
        json_info = self.retrieve_web_data(self.url.rstrip('/') + '/0', attribute='json')
        return int(json_info["messages"][0]["total"])

    def fetch_json_data(self, attempts: int = 0) -> Tuple[int, Query]:
        json_data = self.retrieve_web_data(self.url, attempts=attempts, attribute="json")
        with self._wlock:
            self._result = json_data
        return self.page_number, self

    def execute(self, attempts: int = 0) -> Tuple[int, Query]:
        return self.fetch_json_data(attempts)
