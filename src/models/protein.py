from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.reaction import Reaction


@dataclass
class Protein(Analyte):
    protein_type: str = None


@dataclass
class ProteinReactionRelationship:
    protein: Protein
    reaction: Reaction
    is_reviewed: bool
