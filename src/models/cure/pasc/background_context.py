from dataclasses import dataclass
from typing import Optional

from src.models.cure.pasc.condition import Condition
from src.models.cure.pasc.exposure import Exposure
from src.models.cure.pasc.person import Person
from src.models.node import Node, Relationship


@dataclass
class BackgroundContext(Node):
    immunosuppressant_drugs: Optional[str] = None


@dataclass
class PersonBackgroundContextEdge(Relationship):
    start_node: Person = None
    end_node: BackgroundContext = None


@dataclass
class BackgroundContextConditionEdge(Relationship):
    start_node: BackgroundContext = None
    end_node: Condition = None
    relationship_type: Optional[str] = None


@dataclass
class BackgroundContextExposureEdge(Relationship):
    start_node: BackgroundContext = None
    end_node: Exposure = None
    relationship_type: Optional[str] = None
