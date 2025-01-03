from dataclasses import dataclass, field
from typing import List, Union, Dict

from src.constants import Prefix
from src.interfaces.simple_enum import NodeLabel, RelationshipLabel


@dataclass
class EquivalentId:
    id: str
    type: Prefix
    source: List[str] = None
    status: str = ''

    def type_str(self):
        if hasattr(self.type, 'value'):
            return self.type.value
        return self.type
    def id_str(self):
        return f"{self.type_str()}:{self.id}"

    @staticmethod
    def parse(id: str):
        prefix_part, id_part = id.split(":", 1)
        prefix = Prefix.parse(prefix_part)
        if prefix is None:
            print(f"unknown prefix: {prefix_part}")
        return EquivalentId(id=id_part, type=prefix)

    def __hash__(self):
        # Convert the source list to a tuple for hashing
        return hash((self.id, self.type, tuple(self.source or []), self.status))

    def __eq__(self, other):
        if isinstance(other, EquivalentId):
            return (self.id == other.id and
                    self.type == other.type and
                    set(self.source or []) == set(other.source or []) and
                    self.status == other.status)
        return False


@dataclass
class Node:
    id: str
    labels: List[Union[str, NodeLabel]] = field(default_factory=list)
    xref: List[EquivalentId] = field(default_factory=list)
    provided_by: List[str] = field(default_factory=list)
    field_provenance: Dict[str, Union[str, List[str]]] = field(default_factory=dict)

    def add_label(self, new_label: Union[str, NodeLabel]):
        if new_label not in self.labels:
            self.labels.append(new_label)


@dataclass
class Relationship:
    start_node: Node
    end_node: Node
    labels: List[RelationshipLabel] = field(default_factory=list)
    provided_by: List[str] = field(default_factory=list)
