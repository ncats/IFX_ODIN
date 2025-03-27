from dataclasses import dataclass, field
from typing import List

from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class Ligand(Node):
    name: str = None
    isDrug: bool = None
    smiles: str = None
    description: str = None

@dataclass
class ActivityDetails:
    ref_id: int = None
    act_value: float = None
    act_type: str = None
    action_type: str = None
    has_moa: bool = None
    reference: str = None
    act_pmid: int = None
    moa_pmid: int = None
    act_source: str = None
    moa_source: str = None
    assay_type: str = None
    comment: str = None


@dataclass
class ProteinLigandRelationship(Relationship):
    start_node: Protein
    end_node: Ligand
    meets_idg_cutoff: bool = None
    details: List[ActivityDetails] = field(default_factory=list)
