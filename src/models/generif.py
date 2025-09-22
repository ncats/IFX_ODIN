from dataclasses import dataclass, field
from datetime import datetime

from src.models.gene import Gene
from src.models.node import Node, Relationship


@dataclass
class GeneRif(Node):
    text: str = None


@dataclass
class GeneGeneRifRelationship(Relationship):
    start_node = Gene
    end_node = GeneRif
    gene_id: int = None
    date: datetime = None
    pmids: list[str] = field(default_factory=list)
