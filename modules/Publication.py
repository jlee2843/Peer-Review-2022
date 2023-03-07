from abc import ABC
from threading import Lock
from typing import List, Dict, Tuple, Any

import pyspark
from pyspark.sql import SparkSession


class SimpleClass(ABC):
    _name: str

    def __init__(self, name: str):
        self._name = name

    def get_name(self) -> str:
        return self._name


class MediatorKey(ABC):
    pass


class Department(MediatorKey, SimpleClass):
    pass


class Institution(MediatorKey, SimpleClass):
    pass


class Category(MediatorKey, SimpleClass):
    pass


class Author:
    pass


class Article:
    def get_title(self) -> str:
        pass

    def get_doi(self) -> str:
        pass


class Journal:
    pass


class Publication:
    _journal: Journal
    _article: Article
    _name: str

    def __init__(self, journal: Journal, article: Article):
        self._journal = journal
        self._article = article
        self._name = f'{article.get_title()}\n{article.get_doi()}'


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


class Factory(ABC, metaclass=Singleton):
    _factory_map: Dict[str, Any] = {}
    _lock: Lock = Lock()

    def _create_object(self, identifier: str, class_name: str, *args, **kwargs):
        with self._lock:
            if self._factory_map.get(identifier) is None:
                try:
                    item = getattr(globals(), class_name)(args, kwargs)
                    self._factory_map.update({identifier, item})
                except Exception:
                    raise NameError(f"Class {class_name} is not defined")

    def _get_object(self, identifier: str) -> Any:
        self._factory_map.get(identifier)


class DepartmentFactory(Factory):
    pass


class InstitutionFactory(Factory):
    pass


class AuthorFactory(Factory):
    pass


class CategoryFactory(Factory):
    pass


class ArticleFactory(Factory):
    pass


class JournalFactory(Factory):
    pass


class PublicationFactory(Factory):
    pass


class Mediator(ABC, metaclass=Singleton):
    _mediator_map: Dict[MediatorKey, List[Any]] = {}

    def __init__(self, mediator_key: MediatorKey, item: Any) -> None:
        self._add_item(mediator_key, item)

    def _add_item(self, mediator_key: MediatorKey, item: Any) -> None:
        values: List[Any] = []

        if mediator_key not in self._mediator_map.keys():
            pass
        else:
            values = self._mediator_map.get(mediator_key)

        values.append(item)
        self._mediator_map.update({mediator_key: values})

    def get_nodes_edges(self, sql_ctx: SparkSession, method_name: List[str], relationship: str) -> \
            Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        nodes: List[Tuple] = []
        edges: List[Tuple] = []
        for key in self._mediator_map.keys():
            nodes.append((key, type(key), getattr(key, method_name[0])))
            for item in self._get_value(key):
                nodes.append((item, type(item), getattr(item, method_name[1])))
                edges.append((key, type(item), relationship))

        nodes: pyspark.sql.DataFrame = sql_ctx.createDataFrame(nodes, ['id', 'type', 'name'])
        edges: pyspark.sql.DataFrame = sql_ctx.createDataFrame(edges, ['src', 'dst', 'relationship'])

        return nodes, edges

    def _get_value(self, key: MediatorKey) -> List[Any]:
        return self._mediator_map.get(key)


class InstitutionPublicationMediator(Mediator):
    def __init__(self, institution: Institution, publication: Publication) -> None:
        super().__init__(institution, publication)

    def add_publication(self, institution: Institution, publication: Publication) -> None:
        self._add_item(institution, publication)

    def get_publication(self, institution: Institution) -> List[Publication]:
        return self._get_value(institution)


class DepartmentPublicationMediator(Mediator):
    def __init__(self, department: Department, publication: Publication) -> None:
        super().__init__(department, publication)

    def add_publication(self, dept: Department, publication: Publication) -> None:
        self._add_item(dept, publication)

    def get_publication(self, dept: Department) -> List[Publication]:
        return self._get_value(dept)


class CategoryPublicationMediator(Mediator):
    def __init__(self, category: Category, publication: Publication) -> None:
        super().__init__(category, publication)

    def add_publication(self, category: Category, publication: Publication) -> None:
        self._add_item(category, publication)

    def get_publication(self, category: Category) -> List[Publication]:
        return self._get_value(category)
