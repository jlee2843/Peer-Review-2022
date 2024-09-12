from importlib import import_module
from types import ModuleType
from typing import Dict

from readerwriterlock import rwlock
from sortedcontainers import SortedList

from modules.building_block import *

from abc import ABC
from types import ModuleType
from importlib import import_module
from typing import Any, Dict


class AbstractSingletonFactory(ABC):
    _factory_map: Dict[str, Any] = {}
    _instance = None

    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._lock = rwlock.RWLockFair()
            cls._instance._rlock = cls._instance._lock.gen_rlock()
            cls._instance._wlock = cls._instance._lock.gen_wlock()
        return cls._instance

    def _import_class(self, path: str) -> Any:
        module_path, _, class_name = path.rpartition('.')
        class_: Any
        try:
            module: ModuleType = self._import_module(module_path)
            try:
                class_ = getattr(module, class_name)
            except AttributeError:
                raise RuntimeError(f'Class does not exist: {class_name}')
        except ImportError:
            raise RuntimeError(f'Module does not exist: {module_path}')
        return class_

    def _import_module(self, module_path: str) -> ModuleType:
        """
        Import and return a module defined in the given path.
        """
        return import_module(module_path)

    def create_factory_object(self, identifier: str, class_path: str, *args, **kwargs) -> Any:
        """
        This method creates a factory object using the given identifier, class path, and arguments. It first checks if an
        object with the given identifier already exists. If not, it imports the class and constructs an instance of the class.
        """
        factory_object = self.get_factory_object(identifier)
        if factory_object is None:
            factory_object = self._import_class(class_path)(*args, **kwargs)
            self._update_factory_map(identifier, factory_object)
        return factory_object

    def get_factory_object(self, identifier: str, default=None) -> Any:
        """
        Retrieves the object associated with the given identifier from the factory.
        """
        # Acquire read lock before retrieving object
        with self._rlock:
            return self._factory_map.get(identifier, default)

    def _update_factory_map(self, identifier: str, factory_object: Any):
        """
        Updates the factory map with new factory object and its associated identifier.
        """
        # Acquire write lock before updating map
        with self._wlock:
            self._factory_map[identifier] = factory_object


@dataclass
class ArticleFactory(AbstractSingletonFactory):
    """

    The `ArticleFactory` class is a subclass of `Factory` and is used to create and manage `Article` objects.

    Attributes:
        - `_pub_list` (Set[str]): A set containing the DOIs of the articles in the publication list.

    Methods:
        - `create_base_object(identifier: str, *args, **kwargs) -> Article`: Creates a new `Article` object with the
          given identifier and optional arguments. The object is then added to a `SortedList` associated with the
          identifier in the `_factory_map`
          attribute. Returns the created `Article` object.
        - `get_base_object(identifier: str, **kwargs) -> Optional[Article]`: Retrieves the `Article` object with the
        given identifier
          from the `_factory_map` attribute. Returns `None` if no object is found.
        - `add_publication_list(article: Article) -> None`: Adds the DOI of the given `Article` to the publication list.
        - `get_publication_list() -> List[str]`: Returns a list of DOIs in the publication list.

    """

    @property
    def publication_list(self) -> List[str]:
        with self._rlock:
            return list(self._pub_list)

    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._pub_list = set()
        return cls._instance

    def create_base_object(self, identifier: str, classpath: str, *args, **kwargs) -> Article:
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

        kwargs.update(doi=identifier)
        class_ = self._import_class(classpath)
        base_object: Article = class_(*args, **kwargs)
        articles: SortedList = super().get_factory_object(identifier, SortedList(key=lambda x: x.version))
        articles.add(base_object)
        self._update_factory_map(identifier, articles)
        return base_object

    def get_factory_object(self, identifier: str, **kwargs) -> Optional[Article]:
        result: Optional[SortedList] = super().get_factory_object(identifier)
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
        with self._wlock:
            self._pub_list.add(article.pub_doi)

    def get_publication_list(self) -> List[str]:
        with self._rlock:
            return list(self._pub_list)


@dataclass
class PublicationFactory(AbstractSingletonFactory):
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
    _journal: Optional[Journal] = None
    _article: Optional[Article] = None

    @property
    def journal(self) -> Journal:
        return self._journal

    @property
    def article(self) -> Article:
        return self._article

    def create_factory_object(self, identifier: str, classpath: str, *args, **kwargs) -> Publication:
        publication: Publication = super().create_factory_object(identifier, classpath, *args, **kwargs)
        with self._wlock:
            publication.article = kwargs.get('article')
            publication.journal = kwargs.get('journal')

        return publication


class JournalFactory(AbstractSingletonFactory):
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

    def create_factory_object(self, identifier: str, classpath: str, *args, **kwargs) -> Journal:
        kwargs.update(title=identifier)
        return super().create_factory_object(identifier=identifier, class_path=classpath, *args, **kwargs)


class DepartmentFactory(AbstractSingletonFactory):
    """
    A factory class for creating and retrieving Department objects.

    This class extends the Factory class and provides methods for creating and retrieving Department objects.

    Methods:
        create_object(identifier: str, *args, **kwargs) -> Department:
            Creates a new Department object with the given identifier and additional arguments.

        get_object(identifier: str) -> Department:
            Retrieves the Department object with the given identifier.

    """
    pass


class InstitutionFactory(AbstractSingletonFactory):
    """
    InstitutionFactory Class
    =======================

    This class is a subclass of Factory and is used to create instances of the Institution class.

    Methods
    -------
    create_base_object(identifier: str, *args, **kwargs) -> Institution
        Create a base object with the given identifier.

        Parameters:
        - identifier: str
            A string indicating the identifier of the object.
        - args:
            Positional arguments to be passed to the superclass method.
        - kwargs:
            Keyword arguments to be passed to the superclass method.

        Returns:
        Institution
            An instance of the Institution class.
        return super().create_base_object(identifier, 'modules.building_block.Institution', *args, **kwargs)

    """
    pass


class AuthorFactory(AbstractSingletonFactory):
    """

    AuthorFactory Class
    ===================

    This class provides a factory to create instances of authors. It extends the `Factory` class.

    Methods:
    --------

    create_base_object(identifier: str, *args, **kwargs) -> Author
        Creates a new instance of an `Author` object.

        Parameters:
        - identifier (str): The identifier for the base object.
        - *args: Positional arguments to pass to the `create_base_object()` method of the superclass.
        - **kwargs: Keyword arguments to pass to the `create_base_object()` method of the superclass.

        Returns:
        - Author: The created `Author` object.
        return super().create_base_object(identifier, 'modules.building_block.Author', args, kwargs)

    """
    pass

class CategoryFactory(AbstractSingletonFactory):
    """

    CategoryFactory Class
    =====================

    Subclass of `Factory` that creates `Category` objects.

    Methods
    -------

    create_base_object(identifier: str, *args, **kwargs) -> Category:
        Creates a base `Category` object.

        Parameters
        ----------
        identifier : str
            The identifier for the `Category` object.
        *args : tuple
            Additional positional arguments to pass to the `Category` constructor.
        **kwargs : dict
            Additional keyword arguments to pass to the `Category` constructor.

        Returns
        -------
        Category
            The created `Category` object.

        return super().create_base_object(identifier, 'modules.building_block.Category', *args, **kwargs)
    """
    pass
