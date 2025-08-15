from typing import Generator, List, Union

from sqlalchemy import Column, String, Text, ForeignKey
from sqlalchemy.orm import declarative_base, declared_attr

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.test_models import TestNode, TestRelationship


def create_test_classes():
    class BaseMixin:
        @declared_attr
        def provenance(cls):
            return Column(Text)

    Base = declarative_base(cls=BaseMixin)

    class Node(Base):
        __tablename__ = 'node'
        id = Column(String(18), primary_key=True, nullable=False)
        field_1 = Column(String(50))
        field_2 = Column(String(50))
        field_3 = Column(String(50))

    class Relationship(Base):
        __tablename__ = 'relationship'
        start_node = Column(String(18), ForeignKey('node.id'), primary_key=True, nullable=False)
        end_node = Column(String(18), ForeignKey('node.id'), primary_key=True, nullable=False)
        field_1 = Column(String(50))
        field_2 = Column(String(50))
        field_3 = Column(String(50))

    return {
        'Base': Base,
        'Node': Node,
        'Relationship': Relationship
    }

class TestNodeAdapter(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestNode(id="id_1", field_1="value1", field_2="value2", field_3="value3"),
            TestNode(id="id_2", field_1="value4", field_2="value5")
        ]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="1.0"
        )

class TestNodeAdapter2(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestNode(id="id_3", field_1="value7", field_2="value8", field_3="value9"),
            TestNode(id="id_2", field_1="updated", field_3="first val")
        ]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="2.0"
        )

class TestRelationshipAdapter(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestRelationship(start_node=TestNode(id="id_1"), end_node=TestNode(id="id_2"), field_1="rel_value1"),
            TestRelationship(start_node=TestNode(id="id_2"), end_node=TestNode(id="id_3"), field_2="rel_value2")
        ]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="1.0"
        )


class TestRelationshipAdapter2(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield [
            TestRelationship(start_node=TestNode(id="id_1"), end_node=TestNode(id="id_3"), field_1="rel_value1"),
            TestRelationship(start_node=TestNode(id="id_2"), end_node=TestNode(id="id_3"), field_1="new", field_2="new_rel_value2", field_3="new one also")
        ]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Dummy

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="2.0"
        )