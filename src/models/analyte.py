from dataclasses import dataclass, field
from typing import List, Optional
from src.models.node import Node


@dataclass
class Synonym:
    term: str
    source: str


@dataclass
class Analyte(Node):
    synonyms: Optional[List[Synonym]] = field(default_factory=list)
