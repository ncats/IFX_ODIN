from dataclasses import dataclass
from typing import Optional

from src.core.decorators import search
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class PantherFamily(Node):
    source_id: Optional[str] = None
    level: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None


@dataclass
class PantherFamilyParentEdge(Relationship):
    start_node: PantherFamily
    end_node: PantherFamily


@dataclass
class ProteinPantherFamilyEdge(Relationship):
    start_node: Protein
    end_node: PantherFamily
    source: Optional[str] = None


@dataclass
@search(text_fields=["name", "description"])
class PantherClass(Node):
    source_id: Optional[str] = None
    parent_pcids: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    hierarchy_code: Optional[str] = None


@dataclass
class PantherClassParentEdge(Relationship):
    start_node: PantherClass
    end_node: PantherClass


@dataclass
class ProteinPantherClassEdge(Relationship):
    start_node: Protein
    end_node: PantherClass
    source: Optional[str] = None
