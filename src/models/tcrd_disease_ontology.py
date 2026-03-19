from dataclasses import dataclass
from typing import Optional

from src.models.node import Node, Relationship


@dataclass
class MondoTerm(Node):
    name: Optional[str] = None
    mondo_description: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class MondoTermParentEdge(Relationship):
    start_node: MondoTerm = None
    end_node: MondoTerm = None


@dataclass
class DOTerm(Node):
    name: Optional[str] = None
    do_description: Optional[str] = None


@dataclass
class DOTermParentEdge(Relationship):
    start_node: DOTerm = None
    end_node: DOTerm = None
