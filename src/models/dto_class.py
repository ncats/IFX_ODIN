from dataclasses import dataclass
from typing import Optional

from src.core.decorators import search
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
@search(text_fields=["name", "description"])
class DTOClass(Node):
    source_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None


@dataclass
class DTOClassParentEdge(Relationship):
    start_node: DTOClass
    end_node: DTOClass


@dataclass
class ProteinDTOClassEdge(Relationship):
    start_node: Protein
    end_node: DTOClass
