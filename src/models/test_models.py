from dataclasses import dataclass

from src.models.node import Node, Relationship


@dataclass
class TestNode(Node):
    field_1: str = None
    field_2: str = None
    field_3: str = None

@dataclass
class TestRelationship(Relationship):
    start_node: TestNode
    end_node: TestNode
    field_1: str = None
    field_2: str = None
    field_3: str = None

@dataclass
class AutoIncNode(Node):
    id: str = None
    identifier: str = None
    value: str = None

@dataclass
class TwoKeyAutoIncNode(Node):
    id: str = None
    key1: str = None
    key2: str = None
    value: str = None