from dataclasses import dataclass
from typing import List, Optional

from src.core.decorators import facets
from src.models.analyte import Analyte
from src.models.metabolite_chem_props import MetaboliteChemProps
from src.models.node import Relationship, Node
from src.models.pounce.category_value import CategoryValue
from src.models.protein import Protein
from src.models.reaction import Reaction


@dataclass
@facets(category_fields=['type'])
class Metabolite(Analyte):
    name: Optional[str] = None
    type: Optional[str] = None

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

@dataclass
@facets(category_fields=['type','identification_level'])
class MeasuredMetabolite(Node):
    name: Optional[str] = None
    type: Optional[str] = None
    alternate_ids: Optional[list] = None
    alternate_names: Optional[list] = None
    identification_level: Optional[int] = None
    pathway_ids: Optional[list] = None
    categories: Optional[List[CategoryValue]] = None

@dataclass
class MeasuredMetaboliteEdge(Relationship):
    start_node: MeasuredMetabolite
    end_node: Metabolite