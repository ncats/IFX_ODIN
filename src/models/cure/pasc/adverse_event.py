from dataclasses import dataclass, field
from typing import List, Optional

from src.models.cure.pasc.exposure import Exposure
from src.models.node import Node, Relationship


@dataclass
class AdverseEvent(Node):
    name: Optional[str] = None


@dataclass
class ExposureAdverseEventEdge(Relationship):
    start_node: Exposure = None
    end_node: AdverseEvent = None
    outcomes: List[str] = field(default_factory=list)
