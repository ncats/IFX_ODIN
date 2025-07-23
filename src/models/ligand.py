from dataclasses import dataclass, field, asdict
from typing import List, Optional

from src.core.decorators import facets
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
@facets(category_fields=["isDrug"])
class Ligand(Node):
    name: Optional[str] = None
    isDrug: Optional[bool] = None
    smiles: Optional[str] = None
    description: Optional[str] = None

@dataclass
@facets(category_fields=["act_type", "has_moa", "action_type"],
        numeric_fields=["act_value"])
class ActivityDetails:
    ref_id: Optional[int] = None
    act_value: Optional[float] = None
    act_type: Optional[str] = None
    action_type: Optional[str] = None
    has_moa: Optional[bool] = None
    reference: Optional[str] = None
    act_pmids: Optional[List[int]] = field(default_factory=list)
    moa_pmid: Optional[int] = None
    act_source: Optional[str] = None
    moa_source: Optional[str] = None
    assay_type: Optional[str] = None
    comment: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        # clean data['act_pmids'] if it contains a list of None values
        if 'act_pmids' in data and isinstance(data['act_pmids'], list):
            data['act_pmids'] = [pmid for pmid in data['act_pmids'] if pmid is not None]

        return cls(**data)

@dataclass
@facets(category_fields=["meets_idg_cutoff"])
class ProteinLigandRelationship(Relationship):
    start_node: Protein
    end_node: Ligand
    meets_idg_cutoff: Optional[bool] = None
    details: List[ActivityDetails] = field(default_factory=list)
