from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.node import Relationship
from src.models.reaction import Reaction


@dataclass
class Protein(Analyte):
    protein_type: str = None
    description: str = None
    sequence: str = None


@dataclass
class ProteinReactionRelationship(Relationship):
    start_node: Protein
    end_node: Reaction
    is_reviewed: bool = None
