from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets, search
from src.models.cure.rasopathies.phenotype import Phenotype
from src.models.cure.rasopathies.perinatal_context import PerinatalContext
from src.models.cure.rasopathies.presentation import Presentation
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["finding_context", "group", "label"])
@search(text_fields=["source_value", "source_text", "raw_text"])
class Finding(Node):
    finding_context: Optional[str] = None
    source_value: Optional[str] = None
    source_text: Optional[str] = None
    raw_text: Optional[str] = None
    group: Optional[str] = None
    label: Optional[str] = None
    selected: Optional[bool] = None
    default: Optional[bool] = None


@dataclass
class PresentationFindingEdge(Relationship):
    start_node: Presentation = None
    end_node: Finding = None


@dataclass
class PerinatalContextFindingEdge(Relationship):
    start_node: PerinatalContext = None
    end_node: Finding = None


@dataclass
class FindingPhenotypeEdge(Relationship):
    start_node: Finding = None
    end_node: Phenotype = None
