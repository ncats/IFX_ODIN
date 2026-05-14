from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets
from src.models.cure.pasc.condition import Condition
from src.models.cure.pasc.exposure import Exposure
from src.models.cure.shared.patient import Patient
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=['immunosuppressant_drugs'])
class BackgroundContext(Node):
    immunosuppressant_drugs: Optional[str] = None


@dataclass
class PersonBackgroundContextEdge(Relationship):
    start_node: Patient = None
    end_node: BackgroundContext = None


@dataclass
@facets(category_fields=['relationship_type'])
class BackgroundContextConditionEdge(Relationship):
    start_node: BackgroundContext = None
    end_node: Condition = None
    relationship_type: Optional[str] = None


@dataclass
@facets(category_fields=['relationship_type'])
class BackgroundContextExposureEdge(Relationship):
    start_node: BackgroundContext = None
    end_node: Exposure = None
    relationship_type: Optional[str] = None
