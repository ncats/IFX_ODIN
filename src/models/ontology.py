from dataclasses import dataclass
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship


@dataclass
class Ontology(Node):
    commonName: str = None
    HMDBOntologyType: str = None


@dataclass
class MetaboliteOntologyRelationship(Relationship):
    start_node: Metabolite
    end_node: Ontology
