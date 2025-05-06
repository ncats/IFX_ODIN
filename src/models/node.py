from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import List, Union, get_origin, get_args, Optional

from src.constants import Prefix
from src.interfaces.simple_enum import RelationshipLabel
from src.output_adapters.biolink_labels import BiolinkNodeLabel

def is_datetime_field(type_hint):
    if type_hint is datetime:
        return True
    origin = get_origin(type_hint)
    args = get_args(type_hint)
    return origin in (Union, Optional) and datetime in args

def generate_class_from_dict(cls, data: dict) :
    result = {}
    for f in fields(cls):
        value = data.get(f.name)
        type_hint = f.type

        # Check for Optional[datetime] or datetime
        if is_datetime_field(type_hint) and isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                pass
                # optionally raise or handle differently

        result[f.name] = value

    return cls(**result)

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
    labels: List[BiolinkNodeLabel] = field(default_factory=list)
    xref: List[EquivalentId] = field(default_factory=list)
    provenance: str = None

    def add_label(self, new_label: BiolinkNodeLabel):
        if new_label not in self.labels:
            self.labels.append(new_label)

    @classmethod
    def from_dict(cls, data: dict):
        return generate_class_from_dict(cls, data)


@dataclass
class Relationship:
    start_node: Node
    end_node: Node
    provenance: str = None
    labels: List[RelationshipLabel] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict):
        return generate_class_from_dict(cls, data)
