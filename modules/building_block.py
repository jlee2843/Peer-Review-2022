from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock


class FactoryInstantiationClass(ABC):
    def __init__(self, *args, **kwargs):
        self.__init__(factory=False, *args, **kwargs)

    def __init__(self, factory: bool = False, *args, **kwargs):
        if factory:
            self._create_object(*args, **kwargs)

        else:
            raise RuntimeError('Please instantiate class through the corresponding Factory')

    @abstractmethod
    def _create_object(self, *args, **kwargs):
        pass


class MediatorKey(ABC):
    pass


class Department(MediatorKey, FactoryInstantiationClass):
    pass


class Institution(MediatorKey, FactoryInstantiationClass):
    pass


class Category(MediatorKey, FactoryInstantiationClass):
    pass


class Author(FactoryInstantiationClass):
    pass


class Article(FactoryInstantiationClass):
    def __init__(self, factory: bool = False, *args, **kwargs):
        super().__init__(factory, *args, **kwargs)
        self._authors_detail = None
        self._corr_authors_detail = None
        self._publication_link = None

    def _create_object(self, *args, **kwargs):
        self._doi = kwargs.pop('doi')
        self._title = kwargs.pop('title')
        self._authors = kwargs.pop('authors')
        self._corr_authors = kwargs.pop('corr_authors')
        self._institution = kwargs.pop('institution')
        self._date = kwargs.pop('date')
        self._version = kwargs.pop('version')
        self._type = kwargs.pop('type')
        self._category = kwargs.pop('category')
        self._xml = kwargs.pop('xml')
        self._pub_doi = kwargs.pop('pub_doi')

    def get_title(self) -> str:
        return self._title

    def get_doi(self) -> str:
        return self._doi

    def set_publication_link(self, link: str):
        self._publication_link = link

    def get_publication_link(self) -> str:
        return self._publication_link

    def get_version(self) -> int:
        return self._version

    def get_date(self) -> datetime:
        return self._date


class Journal(FactoryInstantiationClass):
    prefix: str
    title: str
    issn: str
    impact_factor: float

    def create_object(self, prefix: str, title: str, issn: str = None):
        self.prefix = prefix
        self.title = title
        self.issn = issn

    def set_impact_factor(self, impact_factor: float):
        self.impact_factor = impact_factor

    def get_impact_factor(self) -> float:
        return self.impact_factor


class Publication(FactoryInstantiationClass):
    _journal: Journal
    _article: Article
    _name: str
    _id: str

    def __init__(self, journal: Journal, article: Article) -> None:
        super().__init__(True, journal, article)
        self._journal = journal
        self._article = article
        self._name = f'{article.get_title()}\n{article.get_doi()}'
        self._id = article.get_doi()

    def get_article(self) -> Article:
        return self._article

    def set_article(self, article: Article) -> None:
        self._article = article

    def get_journal(self) -> Journal:
        return self._journal

    def set_journal(self, journal: Journal) -> None:
        self._journal = journal


class Singleton(type):
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
