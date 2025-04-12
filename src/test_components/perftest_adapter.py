import random
import string
from typing import Generator, List, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class PerfTestAdapter(InputAdapter):
    field: str
    version: int

    def __init__(self, field: str, version: int):
        InputAdapter.__init__(self)
        self.field = field
        self.version = version

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        nodes = []
        edges = []
        for i in range(100_000):
            node = Node(id=f"node_{i}")
            nodes.append(node)
            self.set_fields(node)

            edge = Relationship(start_node=node, end_node=node)
            self.set_fields(edge)
            edges.append(edge)


        yield [*nodes, *edges]

    def set_fields(self, node):
        setattr(node, self.field, "".join(random.choices(string.ascii_letters, k=10)))
        setattr(node, 'common', "".join(random.choices(string.ascii_letters, k=10)))
        if self.version == 1:
            for j in range(30):
                setattr(node, f"field_{j}", "".join(random.choices(string.ascii_letters, k=10)))

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version=str(self.version))