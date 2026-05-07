from dataclasses import dataclass, field
from typing import List

from src.models.cure.pasc.exposure import Exposure
from src.models.node import Node, Relationship


@dataclass
class Treatment(Node):
    drug_names: List[str] = field(default_factory=list)
    unmatched_drug_names: List[str] = field(default_factory=list)


@dataclass
class TreatmentExposureEdge(Relationship):
    start_node: Exposure = None
    end_node: Treatment = None
