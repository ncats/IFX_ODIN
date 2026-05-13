from dataclasses import asdict, dataclass
from dataclasses import field
from typing import List, Optional

from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class Virus(Node):
    source_id: Optional[str] = None
    nucleic1: Optional[str] = None
    nucleic2: Optional[str] = None
    order: Optional[str] = None
    family: Optional[str] = None
    subfamily: Optional[str] = None
    genus: Optional[str] = None
    species: Optional[str] = None
    name: Optional[str] = None


@dataclass
class ViralProtein(Node):
    source_id: Optional[str] = None
    name: Optional[str] = None
    ncbi: Optional[str] = None


@dataclass
class VirusViralProteinEdge(Relationship):
    start_node: ViralProtein
    end_node: Virus


@dataclass
class ViralPPIDetail:
    source: str
    source_protein_id: Optional[str] = None
    final_lr: Optional[float] = None
    pdb_ids: List[str] = field(default_factory=list)
    high_confidence: Optional[bool] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ViralPPIEdge(Relationship):
    start_node: Protein
    end_node: ViralProtein
    details: List[ViralPPIDetail] = field(default_factory=list)
