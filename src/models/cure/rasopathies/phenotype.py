from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets, search
from src.models.cure.rasopathies.clinical_context import ClinicalContext
from src.models.node import Node, Relationship


@dataclass
@search(text_fields=["name"])
class Phenotype(Node):
    name: Optional[str] = None


@dataclass
@facets(category_fields=["group", "label"])
class ClinicalContextPhenotypeEdge(Relationship):
    start_node: ClinicalContext = None
    end_node: Phenotype = None
    group: Optional[str] = None
    label: Optional[str] = None
    text: Optional[str] = None
    raw_text: Optional[str] = None
    selected: Optional[bool] = None
    default: Optional[bool] = None
