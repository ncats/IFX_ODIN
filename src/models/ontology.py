from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.node import Node, Relationship


@dataclass
class Ontology(Node):
    commonName: str = None
    HMDBOntologyType: str = None


@dataclass
class AnalyteOntologyRelationship(Relationship):
    start_node: Analyte
    end_node: Ontology
