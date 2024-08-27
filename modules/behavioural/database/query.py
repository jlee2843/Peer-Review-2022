from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import RLock
from typing import Tuple, List, Any

from modules.utils.database.connection_processing import get_web_data


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
            self._result = get_web_data(self.cursor, self.url, attr)
        return self.result
