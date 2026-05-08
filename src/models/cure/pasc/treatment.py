from dataclasses import dataclass, field
from typing import List

from src.core.decorators import search, facets
from src.models.cure.pasc.exposure import Exposure
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=['has_unmatched_drug_names'])
@search(text_fields=['drug_names'])
class Treatment(Node):
    drug_names: List[str] = field(default_factory=list)
    unmatched_drug_names: List[str] = field(default_factory=list)
    has_unmatched_drug_names: bool = False


@dataclass
class TreatmentExposureEdge(Relationship):
    start_node: Exposure = None
    end_node: Treatment = None
