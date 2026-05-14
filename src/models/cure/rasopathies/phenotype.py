from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets, search
from src.models.cure.rasopathies.presentation import Presentation
from src.models.node import Node, Relationship


@dataclass
@search(text_fields=["name"])
class Phenotype(Node):
    name: Optional[str] = None


@dataclass
@facets(category_fields=["group", "label"])
class PresentationPhenotypeEdge(Relationship):
    start_node: Presentation = None
    end_node: Phenotype = None
    group: Optional[str] = None
    label: Optional[str] = None
    text: Optional[str] = None
    raw_text: Optional[str] = None
    selected: Optional[bool] = None
    default: Optional[bool] = None
