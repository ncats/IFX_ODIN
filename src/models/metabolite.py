from dataclasses import dataclass
from src.models.analyte import Analyte
from src.models.metabolite_chem_props import MetaboliteChemProps
from src.models.node import Relationship
from src.models.protein import Protein
from src.models.reaction import Reaction


@dataclass
class Metabolite(Analyte):
    name: str = None
    type: str = None,
    identification_level: int = None


@dataclass
class MetaboliteProteinRelationship(Relationship):
    start_node: Metabolite
    end_node: Protein


@dataclass
class MetaboliteReactionRelationship(Relationship):
    start_node: Metabolite
    end_node: Reaction
    substrate_product: int = None
    is_cofactor: bool = None


@dataclass
class MetaboliteChemPropsRelationship(Relationship):
    start_node: Metabolite
    end_node: MetaboliteChemProps
