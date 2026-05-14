from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets, search
from src.models.cure.rasopathies.presentation import Presentation
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["group", "label"])
@search(text_fields=["name", "text"])
class Finding(Node):
    name: Optional[str] = None
    group: Optional[str] = None
    label: Optional[str] = None
    text: Optional[str] = None
    raw_text: Optional[str] = None
    selected: Optional[bool] = None
    default: Optional[bool] = None


@dataclass
class PresentationFindingEdge(Relationship):
    start_node: Presentation = None
    end_node: Finding = None
