import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict, Tuple, Union, Any, Optional, List

import pyspark
from pyspark.sql import SparkSession
from readerwriterlock import rwlock
from sortedcontainers import SortedList, SortedDict

from modules.building_block import MediatorKey, Institution, Publication, Department, Category, Article, \
    InteractionType

logger = logging.getLogger(__name__)


# TODO: need to initiate logger properly

@dataclass
class Mediator(ABC):
    _instance = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._lock: rwlock.RWLockFair = rwlock.RWLockFair()
            cls._rlock: rwlock.RWLockFair._aReader = cls._lock.gen_rlock()
            cls._wlock: rwlock.RWLockFair._aWriter = cls._lock.gen_wlock()
            cls._mediator_map: Dict[Union[MediatorKey, str], Union[SortedList[Any], SortedDict[int, Article]]] = {}
        return cls._instance

    @property
    def mediator_map(self) -> Dict[Union[MediatorKey, str], Union[SortedList[Any], SortedDict[int, Article]]]:
        with self._rlock:
            return self._mediator_map

    @abstractmethod
    def add_object(self, mediator_key: Union[MediatorKey, str], item: Union[SortedList[Any], SortedDict[int, Article]]) \
            -> None:
        self._mediator_map_interaction(InteractionType.ADD, mediator_key, item)

    def get_object(self, key: Union[MediatorKey, str]) -> Optional[Any]:
        return self._mediator_map_interaction(InteractionType.GET, key)

    def get_nodes_edges(self, sql_ctx: SparkSession, method_name: List[str], relationship: str) -> Tuple[
        pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
        nodes: List[Tuple] = []
        edges: List[Tuple] = []
        with self._rlock:
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
                    continue

        nodes_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(nodes, ['id', 'type', 'name'])
        edges_df: pyspark.sql.DataFrame = sql_ctx.createDataFrame(edges, ['src', 'dst', 'relationship'])

        return nodes_df, edges_df

    def _mediator_map_interaction(self, interaction: InteractionType, key: Union[MediatorKey, str],
                                  value: Optional[Union[SortedList[Any], SortedDict[int, Article]]] = None) -> \
            Optional[Union[SortedList[Any], SortedDict[int, Article]]]:
        match interaction:
            case InteractionType.ADD:
                with self._wlock:
                    self._mediator_map[key] = value
            case InteractionType.GET:
                with self._rlock:
                    return self._mediator_map.get(key)


class InstitutionPublicationMediator(Mediator):
    def add_object(self, institution: Institution, publication: Publication, **kwargs) -> None:
        value: SortedList = self.get_object(institution) or SortedList()
        if publication not in value:
            value.add(publication)
            super().add_object(institution, value)

    def get_object(self, institution: Institution) -> Optional[SortedList[Publication]]:
        return super().get_object(institution)


class DepartmentPublicationMediator(Mediator):
    def add_object(self, dept: Department, publication: Publication, **kwargs) -> None:
        value: SortedList = self.get_object(dept) or SortedList()
        if publication not in value:
            value.add(publication)
            super().add_object(dept, value)

    def get_object(self, dept: Department) -> Optional[SortedList[Publication]]:
        return super().get_object(dept)


class CategoryPublicationMediator(Mediator):
    def add_object(self, category: Category, publication: Publication, **kwargs) -> None:
        value: SortedList = self.get_object(category) or SortedList()
        if publication not in value:
            value.add(publication)
            super().add_object(category, value)

    def get_object(self, category: Category) -> Optional[SortedList[Publication]]:
        return super().get_object(category)


class ArticleLinkTypeMediator(Mediator):
    def add_object(self, link_type: str, article: Article, **kwargs) -> None:
        value: SortedList = self.get_object(link_type) or SortedList()
        if article not in value:
            value.add(article)
        super().add_object(link_type, value)

    def get_object(self, link_type: str) -> Optional[SortedList[Article]]:
        return super().get_object(link_type)


class PublishedPrepubArticleMediator(Mediator):
    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._retrieve_initial_prepub_articles: SortedList[str] = SortedList()
        return cls._instance

    def _remove_doi_from_retrieval_list(self, doi: str) -> None:
        self._prepub_article_list_interaction(InteractionType.REMOVE, doi=doi)

    def _add_doi_to_retrieval_list(self, doi: str) -> None:
        self._prepub_article_list_interaction(InteractionType.ADD, doi=doi)

    def _get_retrieve_initial_prepub_articles(self) -> SortedList[str]:
        return self._prepub_article_list_interaction(InteractionType.GET)

    def _prepub_article_list_interaction(self, interaction: InteractionType, doi=None) -> SortedList[str]:
        match interaction:
            case InteractionType.ADD:
                with self._wlock:
                    if doi not in self._retrieve_initial_prepub_articles:
                        self._retrieve_initial_prepub_articles.add(doi)
            case InteractionType.REMOVE:
                with self._wlock:
                    self._retrieve_initial_prepub_articles.discard(doi)
            case InteractionType.GET:
                pass
        with self._rlock:
            return self._retrieve_initial_prepub_articles

    def get_missing_initial_prepub_articles_list(self) -> SortedList[str]:
        return self._get_retrieve_initial_prepub_articles()

    def add_object(self, pub_doi: str, article: Article, **kwargs) -> None:
        article_version_map: Optional[SortedDict[int, Article]] = self.get_object(pub_doi) or SortedDict()
        tmp: Optional[Article] = self.get_article_version(pub_doi, article.version)
        if tmp is None or (article.version <= tmp.version and article.date < tmp.date):
            article_version_map.update({article.version: article})
            super().add_object(pub_doi, article_version_map)

        first_entry: Optional[Article] = self.get_first_stored_article_version(pub_doi)
        if article.version == 1:
            self._remove_doi_from_retrieval_list(pub_doi)
        elif first_entry is not None:
            if first_entry.version != 1:
                self._add_doi_to_retrieval_list(pub_doi)

    def get_article_version(self, pub_doi: str, version: int) -> Optional[Article]:
        tmp: SortedDict[int, Article] = self.get_object(pub_doi)
        return None if tmp is None else tmp.get(version)

    def get_first_stored_article_version(self, pub_doi: str) -> Optional[Article]:
        tmp: SortedDict[int, Article] = self.get_object(pub_doi)

        if tmp is not None:
            tmp1: SortedList = SortedList(tmp.keys())
            return self.get_article_version(pub_doi, tmp1[0])

    def convert_pub_doi_to_doi(self, pub_doi: str) -> str:
        return self.get_first_stored_article_version(pub_doi).doi
