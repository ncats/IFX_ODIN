from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict
from src.models.node import Node


@dataclass
class Synonym:
    term: str
    source: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class Analyte(Node):
    synonyms: Optional[List[Synonym]] = field(default_factory=list)
