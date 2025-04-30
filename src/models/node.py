from dataclasses import dataclass, field, fields
from typing import List, Union, Dict, Any

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
    provenance: str = None
    extra_properties: Dict[str, Any] = field(default_factory=dict)

    def add_label(self, new_label: Union[str, NodeLabel]):
        if new_label not in self.labels:
            self.labels.append(new_label)

    @classmethod
    def from_dict(cls, data: dict):
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)


@dataclass
class Relationship:
    start_node: Node
    end_node: Node
    provenance: str = None
    labels: List[RelationshipLabel] = field(default_factory=list)
