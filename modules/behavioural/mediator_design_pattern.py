import logging
from typing import Dict, Tuple, Union

import pyspark
from pyspark.sql import SparkSession
from sortedcontainers import SortedList

from modules.building_block import *

logger = logging.getLogger(__name__)


class Mediator(metaclass=Singleton):

    def __init__(self):
        self._mediator_map: Dict[Union[MediatorKey, str], Any] = {}
        self._lock: Lock = Lock()

    def add_object(self, mediator_key: Union[MediatorKey, str], item: Any) -> None:
        with self._lock:
            values = self._mediator_map.get(mediator_key, [])
            if item not in values:
                values.append(item)

            self._mediator_map.update({mediator_key: values})

    def get_object(self, key: Union[MediatorKey, str]) -> Optional[Any]:
        with self._lock:
            return self._mediator_map.get(key)

    def get_nodes_edges(self, sql_ctx: SparkSession, method_name: List[str], relationship: str) -> Tuple[
        pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        nodes: List[Tuple] = []
        edges: List[Tuple] = []
        with self._lock:
            for key in self._mediator_map.keys():
                try:
                    nodes.append((key, type(key), getattr(key, method_name[0], None)))
                    for item in self.get_object(key):
                        nodes.append((item, type(item), getattr(item, method_name[1], None)))
                        edges.append((key, type(item), relationship))
                except AttributeError as e:
                    logger.error(f"Error processing key {key} on {method_name[0]} or "
                                 f"item {item} on{method_name[1]} {method_name[1]}: {e}")

        nodes_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(nodes, ['id', 'type', 'name'])
        edges_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(edges, ['src', 'dst', 'relationship'])

        return nodes_df, edges_df


class InstitutionPublicationMediator(Mediator):
    def add_object(self, institution: Institution, publication: Publication) -> None:
        super().add_object(institution, publication)

    def get_object(self, institution: Institution) -> Optional[SortedList[Publication]]:
        return super().get_object(institution)


class DepartmentPublicationMediator(Mediator):
    def add_object(self, dept: Department, publication: Publication) -> None:
        super().add_object(dept, publication)

    def get_object(self, dept: Department) -> Optional[SortedList[Publication]]:
        return super().get_object(dept)


class CategoryPublicationMediator(Mediator):
    def add_object(self, category: Category, publication: Publication) -> None:
        super().add_object(category, publication)

    def get_object(self, category: Category) -> Optional[SortedList[Publication]]:
        return super().get_object(category)


class ArticleLinkTypeMediator(Mediator):
    def add_object(self, link_type: str, article: Article) -> None:
        super().add_object(link_type, article)

    def get_object(self, link_type: str) -> Optional[SortedList[Article]]:
        return super().get_object(link_type)


class PublishedPrepubArticleMediator(Mediator):
    # TODO: need to rethink structure maybe: {pub_doi, {article.version, article}}
    def add_object(self, pub_doi: str, article: Article) -> None:
        with (self._lock):
            add_article: bool = False
            values: Optional[SortedList[Article]] = self.get_article_list(pub_doi)
            if values is None:
                values = SortedList()
                add_article = True
            elif article.version <= values[0].version and article.date < values[0].date:
                add_article = True

            if add_article:
                super().get_object(pub_doi)
                self._mediator_map.update({pub_doi: values})

    def get_article_list(self, pub_doi: str) -> Optional[SortedList[Article]]:
        return super().get_object(pub_doi)

    def get_object(self, pub_doi: str) -> Optional[Article]:
        result = self.get_article_list(pub_doi)
        if result is not None:
            result = result[0]
        return result
