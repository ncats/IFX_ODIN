from dataclasses import dataclass, field
from typing import List


@dataclass
class AnalyteSynonym:
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
    synonyms: List[AnalyteSynonym] = field(default_factory=list)
    equivalent_ids: [EquivalentId] = field(default_factory=list)
