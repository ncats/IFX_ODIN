from dataclasses import dataclass, field
from datetime import datetime

from src.models.gene import Gene
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class GeneRif(Node):
    text: str = None
    pmids: set[str] = field(default_factory=set)


@dataclass
class ProteinGeneRifRelationship(Relationship):
    start_node: Protein
    end_node: GeneRif
    gene_id: int = None
    date: datetime = None

@dataclass
class GeneGeneRifRelationship(Relationship):
    start_node = Gene
    end_node = GeneRif
    gene_id: int = None
    date: datetime = None
