from dataclasses import dataclass, field
from typing import List

from src.core.decorators import facets
from src.models.cure.pasc.exposure import Exposure
from src.models.cure.pasc.phenotype import Phenotype
from src.models.node import Relationship


@dataclass
@facets(category_fields=['outcomes'])
class ExposureAdverseEventEdge(Relationship):
    start_node: Exposure = None
    end_node: Phenotype = None
    outcomes: List[str] = field(default_factory=list)
