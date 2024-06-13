from dataclasses import dataclass
from typing import List


@dataclass
class Synonym:
    term: str
    source: str


@dataclass
class EquivalentId:
    id: str
    type: str
    status: str
    source: str

@dataclass
class Analyte:
    id: str
    synonyms: List[Synonym] = None
    equivalent_ids: [EquivalentId] = None
