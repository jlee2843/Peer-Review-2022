import logging
from abc import abstractmethod, ABC
from threading import Lock
from typing import Dict, Tuple, Union, Any, Optional, List

import pyspark
from pyspark.sql import SparkSession
from sortedcontainers import SortedList, SortedDict

from modules.building_block import Singleton, MediatorKey, Institution, Publication, Department, Category, Article, \
    InteractionType

logger = logging.getLogger(__name__)


# TODO: need to redo since deadlock has occurred.
class Mediator(metaclass=Singleton, ABC):

    def __init__(self):
        self._mediator_map: Dict[Union[MediatorKey, str], Union[SortedList[Any], SortedDict[int, Article]]] = {}
        self._lock: Lock = Lock()

    @abstractmethod
    def add_object(self, mediator_key: Union[MediatorKey, str], item: Any, default: Any = None) -> None:
        if default is None:
            default = SortedList()

        with self._lock:
            values = self._mediator_map.get(mediator_key, default)
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
                    method_attr = getattr(key, method_name[0], None)
                    nodes.append((key, type(key), method_attr))
                    for item in self.get_object(key) or []:
                        item_method_attr = getattr(item, method_name[1], None)
                        nodes.append((item, type(item), item_method_attr))
                        edges.append((key, type(item), relationship))
                except AttributeError as e:
                    logger.error(f"Error processing key {key} on {method_name[0]} or "
                                 f"item {item} on {method_name[1]} {method_name[1]}: {e}")
                    continue
                except TypeError as e:
                    logger.error(f"TypeError: {e}")

        nodes_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(nodes, ['id', 'type', 'name'])
        edges_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(edges, ['src', 'dst', 'relationship'])

        return nodes_df, edges_df

    def _mediator_map_interaction(self, interaction: InteractionType, key: Union[MediatorKey, str],
                                  value: Union[SortedList[Any], SortedDict[int, Article]]) -> \
            Union[SortedList[Any], SortedDict[int, Article], None]:
        with self._lock:
            match interaction:
                case interaction.ADD:
                    if doi not in self._retrieve_initial_prepub_articles:
                        self._retrieve_initial_prepub_articles.add(doi)
                case interaction.REMOVE:
                    self._retrieve_initial_prepub_articles.discard(doi)
                case interaction.GET:
                    return self._mediator_map.get(key)



class InstitutionPublicationMediator(Mediator):
    def add_object(self, institution: Institution, publication: Publication, **kwargs) -> None:
        super().add_object(institution, publication)

    def get_object(self, institution: Institution) -> Optional[SortedList[Publication]]:
        return super().get_object(institution)


class DepartmentPublicationMediator(Mediator):
    def add_object(self, dept: Department, publication: Publication, **kwargs) -> None:
        super().add_object(dept, publication)

    def get_object(self, dept: Department) -> Optional[SortedList[Publication]]:
        return super().get_object(dept)


class CategoryPublicationMediator(Mediator):
    def add_object(self, category: Category, publication: Publication, **kwargs) -> None:
        super().add_object(category, publication)

    def get_object(self, category: Category) -> Optional[SortedList[Publication]]:
        return super().get_object(category)


class ArticleLinkTypeMediator(Mediator):
    def add_object(self, link_type: str, article: Article, **kwargs) -> None:
        super().add_object(link_type, article)

    def get_object(self, link_type: str) -> Optional[SortedList[Article]]:
        return super().get_object(link_type)


class PublishedPrepubArticleMediator(Mediator):
    def __init__(self):
        super().__init__()
        self._retrieve_initial_prepub_articles: SortedList[str] = SortedList()

    def _remove_doi_from_retrieval_list(self, doi: str) -> None:
        self._prepub_article_list_interaction(InteractionType.REMOVE, doi=doi)

    def _add_doi_to_retrieval_list(self, doi: str) -> None:
        self._prepub_article_list_interaction(InteractionType.ADD, doi=doi)

    def _get_retrieve_initial_prepub_articles(self) -> SortedList[str]:
        return self._prepub_article_list_interaction(InteractionType.GET)

    def _prepub_article_list_interaction(self, interaction: InteractionType, doi=None) -> SortedList[str]:
        with self._lock:
            match interaction:
                case interaction.ADD:
                    if doi not in self._retrieve_initial_prepub_articles:
                        self._retrieve_initial_prepub_articles.add(doi)
                case interaction.REMOVE:
                    self._retrieve_initial_prepub_articles.discard(doi)
                case interaction.GET:
                    pass

            return self._retrieve_initial_prepub_articles

    # TODO: need to rethink structure maybe: {pub_doi, {article.version, article}}
    def add_object(self, pub_doi: str, article: Article, **kwargs) -> None:
        with self._lock:
            article_version_map: Optional[SortedDict[int, Article]] = self.get_object(pub_doi) or SortedDict()
            tmp: Article = self.get_article_version(pub_doi, article.version)
            if tmp is None or (article.version <= tmp.version and article.date < tmp.date):
                article_version_map.update({article.version: article})
                self._mediator_map[pub_doi] = article_version_map

            if article.version == 1:
                self._remove_doi_from_retrieval_list(pub_doi)
            else:
                self._add_doi_to_retrieval_list(pub_doi)

    def get_article_version(self, pub_doi: str, version: int) -> Optional[Article]:
        tmp: SortedDict[int, Article] = self.get_object(pub_doi)
        return None if tmp is None else tmp.get(version)
