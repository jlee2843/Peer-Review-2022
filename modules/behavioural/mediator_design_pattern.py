from typing import Dict, List, Any, Tuple, Union

import pyspark
from pyspark.sql import SparkSession

from modules.building_block import *


class Mediator(metaclass=Singleton):
    _mediator_map: Dict[Union[MediatorKey, str], List[Any]] = {}
    _lock: Lock = Lock()

    def add_object(self, mediator_key: Union[MediatorKey, str], item: Any) -> None:
        values: List[Any] = []

        with self._lock:
            values = self._mediator_map.get(mediator_key, [])
            if item not in values:
                values.append(item)

            self._mediator_map.update({mediator_key: values})

    def get_object(self, key: Union[MediatorKey, str]) -> List[Any]:
        return self._mediator_map.get(key)

    def get_nodes_edges(self, sql_ctx: SparkSession, method_name: List[str], relationship: str) -> \
            Tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        nodes: List[Tuple] = []
        edges: List[Tuple] = []
        for key in self._mediator_map.keys():
            nodes.append((key, type(key), getattr(key, method_name[0])))
            for item in self.get_object(key):
                nodes.append((item, type(item), getattr(item, method_name[1])))
                edges.append((key, type(item), relationship))

        nodes: pyspark.sql.DataFrame = sql_ctx.createDataFrame(nodes, ['id', 'type', 'name'])
        edges: pyspark.sql.DataFrame = sql_ctx.createDataFrame(edges, ['src', 'dst', 'relationship'])

        return nodes, edges


class InstitutionPublicationMediator(Mediator):
    def add_object(self, institution: Institution, publication: Publication) -> None:
        super().add_object(institution, publication)

    def get_object(self, institution: Institution) -> List[Publication]:
        return self.get_object(institution)


class DepartmentPublicationMediator(Mediator):
    def add_object(self, dept: Department, publication: Publication) -> None:
        super().add_object(dept, publication)

    def get_object(self, dept: Department) -> List[Publication]:
        return super().get_object(dept)


class CategoryPublicationMediator(Mediator):
    def add_object(self, category: Category, publication: Publication) -> None:
        super().add_object(category, publication)

    def get_object(self, category: Category) -> List[Publication]:
        return super().get_object(category)


class ArticleLinkTypeMediator(Mediator):
    def add_object(self, link_type: str, article: Article) -> None:
        super().add_object(link_type, article)

    def get_object(self, link_type: str) -> List[Article]:
        return super().get_object(link_type)


class PublishedPrepubArticleMediator(Mediator):
    def add_object(self, pub_doi: str, article: Article) -> None:
        values: List[Article] = None

        with self._lock:
            value: Union[Article, None] = self.get_object(pub_doi)
            if value is None or \
                    (value is not None and
                     article.get_version() <= value.get_version() and article.get_date() < value.get_date()):
                value = article

            self._mediator_map.update({pub_doi: [value]})

    def get_object(self, pub_doi: str) -> Article:
        result = self._mediator_map.get(pub_doi)
        if result is not None:
            result = result[0]
        return result
