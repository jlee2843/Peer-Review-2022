from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Optional, List, Any


class NoDefaultValueGiven(Exception):
    pass


@dataclass
class BlockBuilder(ABC):
    """
    BlockBuilder class

    This class is an abstract base class that provides common functionality for building blocks.

    Methods:
    - isvalid(*args, **kwargs): Check if the class was instantiated correctly.
    - get_value(key, default=NoValueGiven, **kwargs): Get the value for a given key from a dictionary of keyword
                                                      arguments.

    Raises:
    - RuntimeError: If the class is instantiated directly without using the corresponding Factory.
    - NoValueGiven: If no value is assigned for a given key.

    """

    @staticmethod
    def isvalid(*args, **kwargs) -> bool:
        if args == () and kwargs == {}:
            raise RuntimeError('Please instantiate class through the corresponding Factory')

        return True

    @staticmethod
    def get_value(key: Any, default: Any = NoDefaultValueGiven, **kwargs) -> Any:
        result = kwargs.get(key, default)
        if isinstance(result, NoDefaultValueGiven):
            raise NoDefaultValueGiven(f'No value was assigned to {key}:')

        return result

    @staticmethod
    def _setattr(obj: object, fields: List[str], default: Any = NoDefaultValueGiven, **kwargs):
        for field in fields:
            setattr(obj, f"_{field}", BlockBuilder.get_value(field, default, **kwargs))


class MediatorKey(ABC):
    pass


@dataclass
class Department(MediatorKey, BlockBuilder):
    def __init__(self, *args, **kwargs):
        """
        Initialize the object.

        :param args: (optional) Variable-length argument list.
        :param kwargs: (optional) Keyword arguments.

        """
        pass


@dataclass
class Institution(MediatorKey, BlockBuilder):
    def __init__(self, *args, **kwargs):
        """
        Initialize the object.

        :param args: (optional) Variable-length argument list.
        :param kwargs: (optional) Keyword arguments.

        """
        pass


@dataclass
class Category(MediatorKey, BlockBuilder):
    def __init__(self, *args, **kwargs):
        """
        Initialize the object.

        :param args: (optional) Variable-length argument list.
        :param kwargs: (optional) Keyword arguments.

        """
        pass


@dataclass
class Author(BlockBuilder):
    def __init__(*args, **kwargs):
        """
        Initialize the object.

        :param args: (optional) Variable-length argument list.
        :param kwargs: (optional) Keyword arguments.

        """
        pass


@dataclass
class Article(BlockBuilder):
    """
    This class represents an Article and is a subclass of BlockBuilder.

    Attributes:
        _corr_authors_detail (Optional[Author]): Optional detailed information about corresponding authors.
        _authors_detail (Optional[List[Author]]): Optional detailed information about authors.
        _publication_link (Optional[str]): Optional link to the publication.

    Methods:
        _create_object(*args, **kwargs): Create an Article object with the given arguments.
        get_title() -> str: Get the title of the Article.
        get_doi() -> str: Get the DOI of the Article.
        set_publication_link(link: str): Set the publication link of the Article.
        get_publication_link() -> str: Get the publication link of the Article.
        get_version() -> int: Get the version of the Article.
        get_date() -> datetime: Get the date of the Article.
        get_pub_doi() -> str: Get the publication DOI of the Article.
    """

    _doi: str
    _title: str
    _authors: str
    _authors: str  # todo: should be a list of Author object(s)
    _corr_authors: str  # todo: should be a list of Author object(s)
    _institution: str  # todo: should be an Institution object
    _date: datetime
    _version: int
    _type: str  # some preprint database assign the type of article it is e.g. new result, contradictory results ...
    _category: List[str]  # article's subject
    _xml: str
    _pub_doi: str
    _corr_authors_detail: Optional[Author]  # todo: remove when Author class has been defined
    _authors_detail: Optional[List[Author]]  # todo: remove when Author class has been defined
    _publication_link: Optional[str]

    def __init__(self, *args, **kwargs):
        super().isvalid(*args, **kwargs)
        # fields that should have a value when the Article class is instantiated
        fields = ['doi', 'title', 'authors', 'corr_authors', 'institution', 'date',
                  'version', 'type', 'category', 'xml', 'pub_doi']
        super()._setattr(self, fields, **kwargs)
        # fields that may not have a value when the Article class is instantiated
        fields = ['authors_detail', 'corr_authors_detail', 'publication_link']
        super()._setattr(self, fields, None, **kwargs)

    @property
    def title(self) -> str:
        return self._title

    @property
    def doi(self) -> str:
        return self._doi

    @property
    def publication_link(self) -> str:
        return self._publication_link

    @publication_link.setter
    def publication_link(self, link: str):
        self._publication_link = link

    @property
    def version(self) -> int:
        return self._version

    @property
    def date(self) -> datetime:
        return self._date

    @property
    def pub_doi(self) -> str:
        return self._pub_doi


@dataclass
class Journal(BlockBuilder):
    """
    Initialize the Journal object with the given parameters.

    :param prefix: A string representing the prefix of the journal (optional).
    :param title: A string representing the title of the journal.
    :param issn: A string representing the ISSN of the journal (optional).
    :param impact_factor: A float representing the impact factor of the journal (optional).
    """
    _prefix: Optional[str]
    _issn: Optional[str]
    _impact_factor: Optional[float]
    _title: str

    def __init__(self, *args, **kwargs):
        super().isvalid(args, kwargs)
        super()._setattr(self, ['title'], **kwargs)
        super()._setattr(self, ['prefix', 'issn'], '', **kwargs)
        super()._setattr(self, ['impact_factor'], 0.0, **kwargs)

    @property
    def impact_factor(self) -> float:
        return self._impact_factor

    @impact_factor.setter
    def impact_factor(self, journal_impact_factor: float):
        self._impact_factor = journal_impact_factor

    @property
    def title(self) -> str:
        return self._title

    @property
    def prefix(self) -> str:
        return self._prefix

    @prefix.setter
    def prefix(self, prefix: str) -> None:
        self._prefix = prefix

    @property
    def issn(self) -> str:
        return self._issn

    @issn.setter
    def issn(self, issn: str) -> None:
        self._issn = issn


class Publication(BlockBuilder):
    _journal: Journal
    _article: Article
    _name: str
    _pub_doi: str

    def __init__(self, *args, **kwargs) -> None:
        super().isvalid(*args, **kwargs)
        super()._setattr(self, ['journal', 'article'], **kwargs)
        article = self._article
        self._name = f'{article.title}\n{article.pub_doi}'
        self._pub_doi = article.pub_doi

    @property
    def article(self) -> Article:
        return self._article

    @article.setter
    def article(self, article: Article) -> None:
        self._article = article

    @property
    def journal(self) -> Journal:
        return self._journal

    @journal.setter
    def journal(self, journal: Journal) -> None:
        self._journal = journal


class Singleton(type):
    """
    Singleton

    A metaclass that implements the Singleton design pattern.

    Attributes:
        _instance (dict): A dictionary that stores the instances of the Singleton classes.
        _lock (Lock): A lock to ensure thread-safe creation of the Singleton instances.

    Methods:
        __call__(*args, **kwargs)
            Return a single instance of the Singleton class.

    Example:
        class MyClass(metaclass=Singleton):
            def __init__(self, value):
                self._value = value

        obj1 = MyClass(1)
        obj2 = MyClass(2)

        print(obj1.value)  # Output: 1
        print(obj2.value)  # Output: 1
        print(obj1 is obj2)  # Output: True
    """
    _instance = {}

    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        # Now, imagine that the program has just been launched. Since there's no
        # Singleton instance yet, multiple threads can simultaneously pass the
        # previous conditional and reach this point almost at the same time. The
        # first of them will acquire lock and will proceed further, while the
        # rest will wait here.
        with cls._lock:
            # The first thread to acquire the lock, reaches this conditional,
            # goes inside and creates the Singleton instance. Once it leaves the
            # lock block, a thread that might have been waiting for the lock
            # release may then enter this section. But since the Singleton field
            # is already initialized, the thread won't create a new object.
            if cls not in cls._instance:
                instance = super().__call__(*args, **kwargs)
                cls._instance[cls] = instance
        return cls._instance[cls]
