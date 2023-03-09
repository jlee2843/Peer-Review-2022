from abc import ABC
from threading import Lock


class FactoryInstantiationClass(ABC):

    def __init__(self, factory: bool = False, *args, **kwargs):
        if factory:
            # create object
            pass
        else:
            raise RuntimeError(f'Please instantiate class through the corresponding Factory')


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
    def get_title(self) -> str:
        pass

    def get_doi(self) -> str:
        pass


class Journal(FactoryInstantiationClass):
    pass


class Publication(FactoryInstantiationClass):
    _journal: Journal
    _article: Article
    _name: str
    _id: str

    def __init__(self, journal: Journal, article: Article) -> None:
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
