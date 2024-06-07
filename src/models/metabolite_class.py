from dataclasses import dataclass
from src.models.metabolite import Metabolite


@dataclass
class MetaboliteClass:
    level: str
    name: str


@dataclass
class MetaboliteClassRelationship:
    metabolite: Metabolite
    met_class: MetaboliteClass
    source: str
