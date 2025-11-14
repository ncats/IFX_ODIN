from abc import ABC
from typing import Generator, List, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.test_models import TestNode, TestRelationship, AutoIncNode, TwoKeyAutoIncNode

class TestAdapterBase(InputAdapter, ABC):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="1.0"
        )


class TestNodeAdapter(TestAdapterBase):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestNode(id="id_1", field_1="value1", field_2="value2", field_3="value3"),
            TestNode(id="id_2", field_1="value4", field_2="value5")
        ]


class TestNodeAdapter2(TestAdapterBase):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestNode(id="id_3", field_1="value7", field_2="value8", field_3="value9"),
            TestNode(id="id_2", field_1="updated", field_3="first val")
        ]

class TestRelationshipAdapter(TestAdapterBase):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestRelationship(start_node=TestNode(id="id_1"), end_node=TestNode(id="id_2"), field_1="rel_value1"),
            TestRelationship(start_node=TestNode(id="id_2"), end_node=TestNode(id="id_3"), field_2="rel_value2")
        ]


class TestRelationshipAdapter2(TestAdapterBase):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestRelationship(start_node=TestNode(id="id_1"), end_node=TestNode(id="id_3"), field_1="rel_value1"),
            TestRelationship(start_node=TestNode(id="id_2"), end_node=TestNode(id="id_3"), field_1="new", field_2="new_rel_value2", field_3="new one also")
        ]


class TestAutoIncNode(TestAdapterBase):

    def get_all(self) -> Generator[List[AutoIncNode], None, None]:
        yield [
            AutoIncNode(identifier="ABC", value="50"),
            AutoIncNode(identifier="DEF", value="502")
        ]


class TestAutoIncNode2(TestAdapterBase):

    def get_all(self) -> Generator[List[AutoIncNode], None, None]:
        yield [
            AutoIncNode(identifier="ABC", value="update"),
            AutoIncNode(identifier="GHI", value="502")
        ]

class TestTwoKeyAutoIncNode(TestAdapterBase):

    def get_all(self) -> Generator[List[TwoKeyAutoIncNode], None, None]:
        yield [
            TwoKeyAutoIncNode(key1="ABC", key2="DEF", value="first"),
            TwoKeyAutoIncNode(key1="ABC", key2="GHI", value="first*")
        ]

class TestTwoKeyAutoIncNode2(TestAdapterBase):

    def get_all(self) -> Generator[List[TwoKeyAutoIncNode], None, None]:
        yield [
            TwoKeyAutoIncNode(key1="ABC", key2="DEF", value="update"),
            TwoKeyAutoIncNode(key1="DEF", key2="GHI", value="nope")
        ]
