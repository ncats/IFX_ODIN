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

        propNode1 = Node(id="prop_1")
        setattr(propNode1, "prop", "hello")

        propNode2 = Node(id="prop_2")
        setattr(propNode2, "prop", "hello")

        relationship = Relationship(start_node=propNode1, end_node=propNode2)
        setattr(relationship, 'field', 'value')
        setattr(relationship, 'list', ['value'])

        relationship2 = Relationship(start_node=propNode2, end_node=propNode1)

        yield [propNode1, propNode2, relationship, relationship2]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version="1")


class SecondAdapter(SimpleAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        propNode1 = Node(id="prop_1")
        setattr(propNode1, "prop", "hello")

        propNode2 = Node(id="prop_2")
        setattr(propNode2, "prop", "goodbye")

        propNode3 = Node(id="prop_3")
        setattr(propNode3, "prop", "goodbye")
        setattr(propNode3, "other", True)

        relationship1 = Relationship(start_node=propNode1, end_node=propNode2)
        setattr(relationship1, 'field', 'new value')
        setattr(relationship1, 'list', ['another value'])
        setattr(relationship1, 'another_field', 'another value')

        relationship2 = Relationship(start_node=propNode2, end_node=propNode3)
        setattr(relationship2, 'field', 'new value')
        setattr(relationship2, 'list', ['another value'])

        yield [propNode2, propNode3, relationship1, relationship2]