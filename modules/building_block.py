from abc import ABC, abstractmethod
from threading import Lock
from typing import List


class FactoryInstantiationClass(ABC):
    def __init__(self):
        self.__init__(factory=False)

    def __init__(self, factory: bool = False, *args, **kwargs):
        if factory:
            self.create_object(*args, **kwargs)

        else:
            raise RuntimeError(f'Please instantiate class through the corresponding Factory')

    @abstractmethod
    def create_object(self, *args, **kwargs):
        pass


class MediatorKey(ABC):
    pass


class Department(MediatorKey, FactoryInstantiationClass):
    def create_object(self, *args, **kwargs):
        pass


class Institution(MediatorKey, FactoryInstantiationClass):
    def create_object(self, *args, **kwargs):
        pass


class Category(MediatorKey, FactoryInstantiationClass):
    def create_object(self, *args, **kwargs):
        pass


class Author(FactoryInstantiationClass):
    pass


class Article(FactoryInstantiationClass):
    _doi: str
    _title: str
    _publication_link: str
    _prepub_link: str
    _primary_author: Author
    _collaborator: List[Author]

    def get_title(self) -> str:
        return self._title

    def get_doi(self) -> str:
        return self._doi

    def create_object(self, doi: str, title: str):
        self._doi = doi
        self._title = title

    def set_publication_link(self, link: str):
        self._publication_link = link

    def get_publication_link(self) -> str:
        return self._publication_link

    def set_prepub_link(self, link: str):
        self._prepub_link = link

    def get_prepub_link(self) -> str:
        return self._prepub_link


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
