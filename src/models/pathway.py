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
    url: Optional[str] = None
    original_datasource: Optional[str] = None


@dataclass
class AnalytePathwayEdge(Relationship):
    start_node: Analyte = None
    end_node: Pathway = None
    source: str = None


@dataclass
class GenePathwayEdge(Relationship):
    start_node: "Gene" = None
    end_node: Pathway = None
    source: str = None


@dataclass
class MetabolitePathwayEdge(Relationship):
    start_node: "Metabolite" = None
    end_node: Pathway = None
    source: str = None


@dataclass
class ProteinPathwayEdge(Relationship):
    start_node: "Protein" = None
    end_node: Pathway = None
    source: str = None


@dataclass
class PathwayParentEdge(Relationship):
    start_node: Pathway = None
    end_node: Pathway = None
    source: str = None
