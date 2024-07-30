from dataclasses import dataclass, field
from typing import List

from src.models.node import Relationship
from src.models.protein import Protein


@dataclass
class PPIRelationship(Relationship):
    start_node: Protein
    end_node: Protein
    sources: List[str] = field(default_factory=list)
    p_int: float = None
    p_ni: float = None
    p_wrong: float = None
    score: float = None
