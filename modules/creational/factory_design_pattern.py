from threading import RLock
from typing import Dict, Any, Set

from sortedcontainers import SortedList

from modules.building_block import *


class Factory(metaclass=Singleton):
    """

    Factory Class
    =============

    The `Factory` class is a singleton class that provides methods for creating and retrieving objects.

    Attributes
    ----------
    _factory_map: Dict[Any, Any]
        A dictionary that maps object identifiers to their corresponding objects.
    _lock: RLock
        A reentrant lock used for thread-safety.

    Methods
    -------
    create_object(identifier: str, class_path: str, *args, **kwargs) -> Any
        Creates an object with the given identifier using the provided class path and optional arguments.
        If an object with the given identifier already exists, it returns the existing object.
        If the object doesn't exist, it creates a new object by dynamically importing the class specified by the class path.

        Parameters:
            identifier (str): The identifier for the object.
            class_path (str): The Python module path and class name, separated by a dot ('.').

        Returns:
            Any: The created or existing object.

    get_object(identifier: str) -> Any
        Retrieves the object with the given identifier.

        Parameters:
            identifier (str): The identifier for the object.

        Returns:
            Any: The object with the given identifier.

    import_class(path: str) -> object
        Dynamically imports a class based on the provided class path.

        Parameters:
            path (str): The Python module path and class name, separated by a dot ('.').

        Returns:
            object: The imported class object.

    """

    _factory_map: Dict[Any, Any] = {}
    _lock: RLock = RLock()

    def create_object(self, identifier: str, class_path: str, *args, **kwargs) -> Any:
        """
        Create and return a new object based on the provided identifier and class path.

        :param identifier: A string representing the identifier for the object.
        :param class_path: A string representing the fully qualified class path for the object.
        :param args: Additional positional arguments to be passed to the object constructor.
        :param kwargs: Additional keyword arguments to be passed to the object constructor.
        :return: A new object based on the provided identifier and class path.
        """

        with self._lock:
            # kwargs.update(doi=identifier)
            new_object = self._factory_map.get(identifier)

            if new_object is None:
                new_object = Factory.import_class(class_path)(*args, **kwargs)
                self._factory_map[identifier] = new_object

        return new_object

    def get_object(self, identifier: str) -> Any:
        """
        The get_object function is a method of the Factory class (and its subclasses). It takes an identifier as input
        and returns the corresponding object.

        :param identifier: The unique identifier of the object to be retrieved.
        :return: The object corresponding to the provided identifier.
        """

        return self._factory_map.get(identifier)

    @staticmethod
    def import_class(path: str) -> object:
        """
        Import and return a class defined in the given path.

        :param path: The full path of the class.
        :return: The imported class object.
        """

        from importlib import import_module
        module_path, _, class_name = path.rpartition('.')
        mod = import_module(module_path)
        klass: object = getattr(mod, class_name)
        return klass


class DepartmentFactory(Factory):
    """
    A factory class for creating and retrieving Department objects.

    This class extends the Factory class and provides methods for creating and retrieving Department objects.

    Methods:
        create_object(identifier: str, *args, **kwargs) -> Department:
            Creates a new Department object with the given identifier and additional arguments.

        get_object(identifier: str) -> Department:
            Retrieves the Department object with the given identifier.

    """

    def create_object(self, identifier: str, *args, **kwargs) -> Department:
        return super().create_object(identifier, "modules.building_block.Department", *args, **kwargs)

    def get_object(self, identifier: str) -> Department:
        return super().get_object(identifier)


class InstitutionFactory(Factory):
    """

    The `InstitutionFactory` class is a subclass of the `Factory` class. It provides the functionality to create and
    retrieve `Institution` objects.

    Methods:
    ---------
    - `create_object(identifier: str, *args, **kwargs) -> Institution`:
        This method creates a new `Institution` object based on the given identifier. It calls the `create_object`
        method of the superclass with the identifier and additional arguments and keyword arguments. It returns the
        created `Institution` object.

    - `get_object(identifier: str) -> Institution`:
        This method retrieves an existing `Institution` object based on the given identifier. It calls the `get_object`
        method of the superclass with the identifier. It returns the retrieved`Institution` object.

    """
    def create_object(self, identifier: str, *args, **kwargs) -> Institution:
        return super().create_object(identifier, 'modules.building_block.Institution', *args, **kwargs)

    def get_object(self, identifier: str) -> Institution:
        return super().get_object(identifier)


class AuthorFactory(Factory):
    """
    A factory class for creating and retrieving Author objects.

    This class extends the base Factory class.

    Methods:
        create_object(identifier: str, *args, **kwargs) -> Author
            Create a new Author object with the given identifier, and optional arguments and keyword arguments.

        get_object(identifier: str) -> Author
            Retrieve an existing Author object with the given identifier.
    """
    def create_object(self, identifier: str, *args, **kwargs) -> Author:
        return super().create_object(identifier, 'modules.building_block.Author', args, kwargs)

    def get_object(self, identifier: str) -> Author:
        return super().get_object(identifier)


class CategoryFactory(Factory):
    """
    Class CategoryFactory

    This class extends the Factory class and provides methods for creating and retrieving Category objects.

    Methods:
        - `create_object(identifier: str, *args, **kwargs) -> Category`: creates a new Category object with the given
          identifier and additional arguments and keyword arguments.

        - `get_object(identifier: str) -> Category`: retrieves an existing Category object with the given identifier.

    """
    def create_object(self, identifier: str, *args, **kwargs) -> Category:
        return super().create_object(identifier, 'modules.building_block.Category', *args, **kwargs)

    def get_object(self, identifier: str) -> Category:
        return super().get_object(identifier)


class ArticleFactory(Factory):
    """
    ArticleFactory Class
    ====================

    This class represents a factory for creating and managing Article objects.

    Attributes:
    -----------
        _pub_list (Set[str]): A Set object to store publication DOIs.

    Methods:
    --------
        create_object(identifier: str, *args, **kwargs) -> Article:
            Create a new Article object with the given identifier and arguments.

        get_object(identifier: str) -> Optional[Article]:
            Return the Article object with the given identifier if it exists, otherwise return None.

        add_publication_list(article: Article):
            Add the publication DOI of the given Article object to the publication list.

        get_publication_list() -> List[str]:
            Return a list of publication DOIs in the publication list.
    """
    def __init__(self):
        self._pub_list: Set[str] = set()

    def create_object(self, identifier: str, *args, **kwargs) -> Article:
        """
        :param identifier: A string representing the identifier of the object to be created.
        :param args: Additional positional arguments to be passed to the object's constructor.
        :param kwargs: Additional keyword arguments to be passed to the object's constructor.
        :return: The created object of type Article.

        This method creates a new object of type Article with the given identifier and optional arguments in the .
        The identifier parameter is used to update the 'doi' keyword argument in kwargs. The new object is then added
        to a SortedList associated with the identifier in the _factory_map attribute of the
        calling object. Finally, the new object is returned.
        """

        with self._lock:
            kwargs.update(doi=identifier)
            new_object = Factory.import_class('modules.building_block.Article')(*args, **kwargs)
            articles: SortedList = self._factory_map.get(identifier, SortedList(key=lambda x: x.version))
            articles.add(new_object)
            self._factory_map[identifier] = articles

        return new_object

    def get_object(self, identifier: str) -> Optional[Article]:
        result: Optional[SortedList] = self._factory_map.get(identifier)
        if result is not None:
            result = result.__getitem__(0)

        return result

    def add_publication_list(self, article: Article) -> None:
        """
        Add the articles DOI to the publication list.

        :param article: The article to be added to the publication list.
        :type article: Article
        :return: None
        """
        with self._lock:
            pub_doi = article.pub_doi
            if len(self._pub_list) == 0:
                self._pub_list: Set[str] = {pub_doi}
            else:
                self._pub_list.add(pub_doi)

    def get_publication_list(self) -> List[str]:
        return list(self._pub_list)


class JournalFactory(Factory):
    """
    JournalFactory

    Class for creating and retrieving Journal objects.

    Methods:
        * create_object(identifier: str, *args, **kwargs) -> Journal
            Method for creating a new Journal object.

            Parameters:
                - identifier: str
                    The identifier of the Journal.

            Returns:
                - Journal
                    The created Journal object.

        * get_object(identifier: str) -> Journal
            Method for retrieving a Journal object.

            Parameters:
                - identifier: str
                    The identifier of the Journal.

            Returns:
                - Journal
                    The retrieved Journal object.
    """

    def create_object(self, identifier: str, *args, **kwargs) -> Journal:
        kwargs.update(title=identifier)
        return super().create_object(identifier, 'modules.building_block.Journal', *args, **kwargs)

    def get_object(self, identifier: str) -> Journal:
        return super().get_object(identifier)


class PublicationFactory(Factory):
    """
    The `PublicationFactory` class is a subclass of the `Factory` class. It provides methods for creating and
    retrieving `Publication` objects.

    Methods:
    ---------

    `create_object(self, identifier: str, *args, **kwargs) -> Publication`:
        This method creates a new `Publication` object with the given identifier and any additional arguments and
        keyword arguments. It calls the `create_object` method of the parent `Factory` class, passing in the
        identifier, the class name `modules.building_block.Publication`, and the additional arguments and keyword
        arguments. The created `Publication` object is then returned.

        Parameters:
            - `identifier` (str): The identifier for the new `Publication` object.
            - `*args` (tuple): Additional arguments to be passed to the `create_object` method.
            - `**kwargs` (dict): Keyword arguments to be passed to the `create_object` method.

        Returns:
            - `Publication`: The created `Publication` object.

    `get_object(self, identifier: str) -> Publication`:
        This method retrieves an existing `Publication` object with the given identifier. It calls the `get_object`
        method of the parent `Factory` class, passing in the identifier. The retrieved `Publication` object is
        then returned.

        Parameters:
            - `identifier` (str): The identifier of the `Publication` object to retrieve.

        Returns:
            - `Publication`: The retrieved `Publication` object.

    """
    def create_object(self, identifier: str, *args, **kwargs) -> Publication:
        return super().create_object(identifier, 'modules.building_block.Publication', *args, **kwargs)

    def get_object(self, identifier: str) -> Publication:
        return super().get_object(identifier)
