from typing import Generator, List, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class SimpleAdapter(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        nodeA = Node(id="A")
        nodeB = Node(id="B")
        nodeC = Node(id="unmatched")
        relationship = Relationship(start_node=nodeA, end_node=nodeB)
        unmatched_rel = Relationship(start_node=nodeA, end_node=nodeC)
        unmatched_rel2 = Relationship(start_node=nodeC, end_node=nodeB)
        yield from [
            [nodeA, nodeB, nodeC,
             relationship,
             unmatched_rel, unmatched_rel2]
        ]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version="1")