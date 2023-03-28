from typing import Dict, Any

from modules.building_block import *


class Factory(metaclass=Singleton):
    _factory_map: Dict[Any, Any] = {}
    _lock: Lock = Lock()

    def create_object(self, identifier: Any, class_name: str, *args, **kwargs) -> Any:
        new_object = getattr(globals(), class_name)(True, identifier, *args, **kwargs)

        if self.add_object(identifier, new_object) is False:
            new_object = self.get_object(identifier)

        return new_object

    def add_object(self, identifier: Any, new_object: Any) -> bool:
        result = False
        with self._lock:
            if self._factory_map.get(identifier) is None:
                result = True
                self._factory_map.update({identifier, new_object})

            return result

    def get_object(self, identifier: Any) -> Any:
        with self._lock:
            return self._factory_map.get(identifier)


class DepartmentFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Department:
        return super().create_object(identifier, "Department", args, kwargs)

    def get_object(self, identifier: str) -> Department:
        return super().get_object(identifier)


class InstitutionFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Institution:
        return super().create_object(identifier, 'Institution', args, kwargs)

    def get_object(self, identifier: str) -> Institution:
        return super().get_object(identifier)


class AuthorFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Author:
        return super().create_object(identifier, 'Author', args, kwargs)

    def get_object(self, identifier: str) -> Author:
        return super().get_object(identifier)


class CategoryFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Category:
        return super().create_object(identifier, 'Category', args, kwargs)

    def get_object(self, identifier: str) -> Category:
        return super().get_object(identifier)


class ArticleFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Article:
        return super().create_object(identifier=identifier, class_name='Article', *args, **kwargs)

    def get_object(self, identifier: str) -> Article:
        return super().get_object(identifier)


class JournalFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Journal:
        return super().create_object(identifier, 'Journal', args, kwargs)

    def get_object(self, identifier: str) -> Journal:
        return super().get_object(identifier)


class PublicationFactory(Factory):
    def create_object(self, identifier: str, *args, **kwargs) -> Publication:
        return super().create_object(identifier, 'Publication', args, kwargs)

    def get_object(self, identifier: str) -> Publication:
        return super().get_object(identifier)
