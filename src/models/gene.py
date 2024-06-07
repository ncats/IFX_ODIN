from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.reaction import Reaction


@dataclass
class Gene(Analyte):
    protein_type: str = ''


@dataclass
class ProteinReactionRelationship:
    gene: Gene
    reaction: Reaction
    is_reviewed: bool
