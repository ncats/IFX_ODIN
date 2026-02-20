from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from src.models.analyte import Analyte
from src.models.node import Node, Relationship

if TYPE_CHECKING:
    from src.models.gene import Gene
    from src.models.metabolite import Metabolite
    from src.models.protein import Protein


@dataclass
class Pathway(Node):
    source_id: str = None
    type: str = None
    category: Optional[str] = None
    name: str = None


@dataclass
class AnalytePathwayRelationship(Relationship):
    start_node: Analyte = None
    end_node: Pathway = None
    source: str = None


@dataclass
class GenePathwayRelationship(Relationship):
    start_node: "Gene" = None
    end_node: Pathway = None
    source: str = None


@dataclass
class MetabolitePathwayRelationship(Relationship):
    start_node: "Metabolite" = None
    end_node: Pathway = None
    source: str = None


@dataclass
class ProteinPathwayRelationship(Relationship):
    start_node: "Protein" = None
    end_node: Pathway = None
    source: str = None
