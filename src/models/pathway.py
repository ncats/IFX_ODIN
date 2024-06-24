from dataclasses import dataclass

from src.models.analyte import Analyte
from src.models.node import Node, Relationship


@dataclass
class Pathway(Node):
    source_id: str = None
    type: str = None
    category: str = None
    name: str = None


@dataclass
class AnalytePathwayRelationship(Relationship):
    start_node: Analyte
    end_node: Pathway
    source: str = None
