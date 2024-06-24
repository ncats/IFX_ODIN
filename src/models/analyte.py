from dataclasses import dataclass, field
from typing import List
from src.models.node import Node


@dataclass
class Synonym:
    term: str
    source: str


@dataclass
class Analyte(Node):
    synonyms: List[Synonym] = field(default_factory=list)
