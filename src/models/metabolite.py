from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.metabolite_chem_props import MetaboliteChemProps
from src.models.protein import Protein
from src.models.reaction import Reaction


@dataclass
class Metabolite(Analyte):
    pass


@dataclass
class MetaboliteProteinRelationship:
    metabolite: Metabolite
    protein: Protein


@dataclass
class MetaboliteReactionRelationship:
    metabolite: Metabolite
    reaction: Reaction
    substrate_product: int
    is_cofactor: bool


@dataclass
class MetaboliteChemPropsRelationship:
    metabolite: Metabolite
    chem_prop: MetaboliteChemProps
