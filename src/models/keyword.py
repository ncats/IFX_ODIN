from dataclasses import dataclass
from typing import Optional

from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class Keyword(Node):
    category: Optional[str] = None
    source: Optional[str] = None
    value: Optional[str] = None


@dataclass
class ProteinKeywordEdge(Relationship):
    start_node: Protein
    end_node: Keyword

