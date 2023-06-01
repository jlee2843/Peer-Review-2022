from typing import Dict, Any, Set

from sortedcontainers import SortedList

from modules.building_block import *


class Factory(metaclass=Singleton):
    _factory_map: Dict[Any, Any] = {}
    _lock: Lock = Lock()

    def create_object(self, identifier: str, class_path: str, *args, **kwargs) -> Any:
        with self._lock:
            kwargs.update(doi=identifier)
            new_object = Factory.import_class(class_path)(True, *args, **kwargs)

            if self._add_object(identifier, new_object) is False:
                new_object = self.get_object(identifier)

        return new_object

    def _add_object(self, identifier: str, new_object: Any) -> bool:
        result = False
        with self._lock:
            if self._factory_map.get(identifier) is None:
                result = True
                self._factory_map[identifier] = new_object

            return result

    def get_object(self, identifier: str) -> Any:
        with self._lock:
            return self._factory_map.get(identifier)

    @staticmethod
    def import_class(path: str) -> object:
        from importlib import import_module
        module_path, _, class_name = path.rpartition('.')
        mod = import_module(module_path)
        klass: object = getattr(mod, class_name)
        return klass


class DepartmentFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Department:
        return super().create_object(identifier, "modules.building_block.Department", *args, **kwargs)

    def get_object(self, identifier: str) -> Department:
        return super().get_object(identifier)


class InstitutionFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Institution:
        return super().create_object(identifier, 'modules.building_block.Institution', *args, **kwargs)

    def get_object(self, identifier: str) -> Institution:
        return super().get_object(identifier)


class AuthorFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Author:
        return super().create_object(identifier, 'modules.building_block.Author', args, kwargs)

    def get_object(self, identifier: str) -> Author:
        return super().get_object(identifier)


class CategoryFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Category:
        return super().create_object(identifier, 'modules.building_block.Category', *args, **kwargs)

    def get_object(self, identifier: str) -> Category:
        return super().get_object(identifier)


class ArticleFactory(Factory):
    def __init__(self):
        self._pub_list: Set[str] = set()

    def create_object(self, identifier: str, *args, **kwargs) -> Article:
        with self._lock:
            kwargs.update(doi=identifier)
            new_object = Factory.import_class('modules.building_block.Article')(True, *args, **kwargs)
            articles: SortedList = self._factory_map.get(identifier, SortedList(key=lambda x: x.get_version()))
            articles.add(new_object)
            self._factory_map[identifier] = articles

        return new_object

    def get_object(self, identifier: str) -> Optional[Article]:
        result: Optional[SortedList] = self._factory_map.get(identifier)
        if result is not None:
            result = result.__getitem__(0)
        return result

    def add_publication_list(self, article: Article):
        with self._lock:
            pub_doi = article.get_pub_doi()
            if len(self._pub_list) == 0:
                self._pub_list: Set[str] = {pub_doi}
            else:
                self._pub_list.add(pub_doi)

    def get_publication_list(self) -> List[str]:
        return list(self._pub_list)


class JournalFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Journal:
        """

        :type identifier: str
        """
        kwargs.update(title=identifier)
        return super().create_object(identifier, 'modules.building_block.Journal', *args, **kwargs)

    def get_object(self, identifier: str) -> Journal:
        return super().get_object(identifier)


class PublicationFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Publication:
        return super().create_object(identifier, 'modules.building_block.Publication', *args, **kwargs)

    def get_object(self, identifier: str) -> Publication:
        return super().get_object(identifier)
