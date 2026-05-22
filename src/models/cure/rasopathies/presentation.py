from dataclasses import dataclass

from src.models.cure.pasc.condition import Condition
from src.models.cure.shared.patient import Patient
from src.models.node import Node, Relationship


@dataclass
class Presentation(Node):
    pass


@dataclass
class PatientPresentationEdge(Relationship):
    start_node: Patient = None
    end_node: Presentation = None


@dataclass
class PresentationConditionEdge(Relationship):
    start_node: Presentation = None
    end_node: Condition = None
