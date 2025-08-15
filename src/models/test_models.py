from dataclasses import dataclass

from src.models.node import Node, Relationship


@dataclass
class TestNode(Node):
    id: str
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

