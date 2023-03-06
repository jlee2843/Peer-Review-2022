from typing import List, Dict

import pyspark


class Department:
    pass


class Institution:
    pass


class Author:
    pass


class Article:
    pass


class Journal:
    pass


class Publication:
    pass


class Singleton(type):
    from threading import Lock

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


class InstitutionPublicationMediator(metaclass=Singleton):
    institution_publication_map: Dict[Institution, List[Publication]] = {}

    def __init__(self, institution: Institution, publication: Publication) -> None:
        values: List[Publication] = []

        if institution in self.institution_publication_map.keys():
            values = self.institution_publication_map.get(institution)

        values.append(publication)
        self.institution_publication_map.update({institution: values})

    def get_map(self) -> institution_publication_map:
        return self.institution_publication_map

    #def get_nodes(self, sc:pyspark.SparkContext) -> pyspark.SparkContext:
class DepartmentPublicationMediator(metaclass=Singleton):
    department_publication_map: Dict[Department, List[Publication]] = {}

    def __init__(self, department: Department, publication: Publication) -> None:
        values: List[Publication] = []

        if department in self.department_publication_map.keys():
            values = self.department_publication_map.get(department)

        values.append(publication)
        self.department_publication_map.update({department: values})
