from dataclasses import dataclass, field, asdict
from typing import List, Optional

from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class Ligand(Node):
    name: Optional[str] = None
    isDrug: Optional[bool] = None
    smiles: Optional[str] = None
    description: Optional[str] = None

@dataclass
class ActivityDetails:
    ref_id: Optional[int] = None
    act_value: Optional[float] = None
    act_type: Optional[str] = None
    action_type: Optional[str] = None
    has_moa: Optional[bool] = None
    reference: Optional[str] = None
    act_pmid: Optional[int] = None
    moa_pmid: Optional[int] = None
    act_source: Optional[str] = None
    moa_source: Optional[str] = None
    assay_type: Optional[str] = None
    comment: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

@dataclass
class ProteinLigandRelationship(Relationship):
    start_node: Protein
    end_node: Ligand
    meets_idg_cutoff: Optional[bool] = None
    details: List[ActivityDetails] = field(default_factory=list)
