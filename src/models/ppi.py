from dataclasses import dataclass, field
from typing import List

from src.models.node import Relationship
from src.models.protein import Protein


@dataclass
class PPIEdge(Relationship):
    start_node: Protein
    end_node: Protein
    sources: List[str] = field(default_factory=list)
    p_int: List[float] = field(default_factory=list)
    p_ni: List[float] = field(default_factory=list)
    p_wrong: List[float] = field(default_factory=list)
    pmids: List[int] = field(default_factory=list)
    contexts: List[str] = field(default_factory=list)
    interaction_type: List[str] = field(default_factory=list)
    score: List[float] = field(default_factory=list)
