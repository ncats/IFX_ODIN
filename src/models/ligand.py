from dataclasses import dataclass, field, asdict
from typing import List, Dict

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

    def to_dict(self) -> Dict[str, List[str]]:
        ret_dict = {}
        for key, value in asdict(self).items():
            if value not in [None, '', [], {}]:
                ret_dict[key] = [value]
        return ret_dict

@dataclass
class ProteinLigandRelationship(Relationship):
    start_node: Protein
    end_node: Ligand
    details: ActivityDetails = None