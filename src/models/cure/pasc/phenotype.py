from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets
from src.models.cure.pasc.episode import Episode
from src.models.node import Node, Relationship


@dataclass
class Phenotype(Node):
    name: Optional[str] = None
    short_name: Optional[str] = None


@dataclass
@facets(category_fields=['severity'])
class EpisodePhenotypeEdge(Relationship):
    start_node: Episode = None
    end_node: Phenotype = None
    severity: Optional[str] = None
