from dataclasses import dataclass
from datetime import datetime

from src.models.node import Node, Relationship


@dataclass
class DatabaseVersion(Node):
    timestamp: datetime = None
    notes: str = None


@dataclass
class DataVersion(Node):
    name: str = None
    url: str = None
    version: str = None


@dataclass
class DatabaseDataVersionRelationship(Relationship):
    start_node: DatabaseVersion
    end_node: DataVersion
