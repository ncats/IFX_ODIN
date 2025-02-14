from dataclasses import dataclass
from datetime import datetime
from typing import List

from src.models.gene import Gene
from src.models.node import Node, Relationship


@dataclass
class Disease(Node):
    name: str = None

@dataclass
class GeneDiseaseRelationship(Relationship):
    start_node: Gene = None
    end_node: Disease = None
    types: List[str] = None
    evidence_codes: List[str] = None
    evidence_terms: List[str] = None
    references: List[str] = None
    dates: List[datetime] = None
    sources: List[str] = None
