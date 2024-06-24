from dataclasses import dataclass, field
from typing import List, Union

from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel


@dataclass
class EquivalentId:
    id: str
    type: str
    status: str
    source: str


@dataclass
class Node:
    id: str
    labels: List[Union[str, NodeLabel]]
    equivalent_ids: List[EquivalentId] = field(default_factory=list)
    provenance: List[str] = field(default_factory=list)

    def add_label(self, new_label: Union[str, NodeLabel]):
        if new_label not in self.labels:
            self.labels.append(new_label)


@dataclass
class Relationship:
    start_node: Node
    end_node: Node
    labels: List[RelationshipLabel]
    provenance: List[str] = field(default_factory=list)
