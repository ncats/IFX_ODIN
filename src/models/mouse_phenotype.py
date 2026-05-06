from dataclasses import dataclass, field, asdict
from typing import List, Optional

from src.core.decorators import search
from src.models.gene import Gene
from src.models.node import Node, Relationship
from src.models.ortholog import OrthologGene
from src.models.protein import Protein


@dataclass
@search(text_fields=["name"])
class MousePhenotype(Node):
    name: Optional[str] = None


@dataclass
class MousePhenotypeDetail:
    source: str
    source_id: Optional[str] = None
    top_level_term_id: Optional[str] = None
    top_level_term_name: Optional[str] = None
    trait: Optional[str] = None
    p_value: Optional[float] = None
    percentage_change: Optional[str] = None
    effect_size: Optional[str] = None
    procedure_name: Optional[str] = None
    parameter_name: Optional[str] = None
    gp_assoc: Optional[bool] = None
    statistical_method: Optional[str] = None
    sex: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class OrthologGeneMousePhenotypeEdge(Relationship):
    start_node: OrthologGene = None
    end_node: MousePhenotype = None
    details: List[MousePhenotypeDetail] = field(default_factory=list)


@dataclass
class GeneMousePhenotypeEdge(Relationship):
    start_node: Gene = None
    end_node: MousePhenotype = None
    details: List[MousePhenotypeDetail] = field(default_factory=list)


@dataclass
class ProteinMousePhenotypeEdge(Relationship):
    start_node: Protein = None
    end_node: MousePhenotype = None
    details: List[MousePhenotypeDetail] = field(default_factory=list)
