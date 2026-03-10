from dataclasses import dataclass, field
from typing import List, Optional
from src.models.node import Node, Relationship
from src.core.decorators import facets

@dataclass
@facets(category_fields=["name"])
class Tissue(Node):
    name: Optional[str] = None
    definition: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)

@dataclass
class TissueParentEdge(Relationship):
    start_node: Tissue
    end_node: Tissue
