from dataclasses import dataclass

from src.models.cure.pasc.condition import Condition
from src.models.cure.shared.patient import Patient
from src.models.node import Node, Relationship


@dataclass
class ClinicalContext(Node):
    pass


@dataclass
class PatientClinicalContextEdge(Relationship):
    start_node: Patient = None
    end_node: ClinicalContext = None


@dataclass
class ClinicalContextConditionEdge(Relationship):
    start_node: ClinicalContext = None
    end_node: Condition = None
