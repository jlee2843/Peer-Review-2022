from modules.building_block import *


class Mediator:
    _mediator_map: Dict[MediatorKey, List[Any]] = {}

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


class InstitutionPublicationMediator(Mediator, metaclass=Singleton):
    def add_publication(self, institution: Institution, publication: Publication) -> None:
        super()._add_item(institution, publication)

    def get_publication(self, institution: Institution) -> List[Publication]:
        return self._get_value(institution)


class DepartmentPublicationMediator(Mediator, metaclass=Singleton):
    def add_publication(self, dept: Department, publication: Publication) -> None:
        super()._add_item(dept, publication)

    def get_publication(self, dept: Department) -> List[Publication]:
        return self._get_value(dept)


class CategoryPublicationMediator(Mediator, metaclass=Singleton):
    def add_publication(self, category: Category, publication: Publication) -> None:
        self._add_item(category, publication)

    def get_publication(self, category: Category) -> List[Publication]:
        return self._get_value(category)
