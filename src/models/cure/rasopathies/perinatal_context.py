from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets
from src.models.cure.shared.patient import Patient
from src.models.cure.rasopathies.phenotype import Phenotype
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["premature_birth"])
class PerinatalContext(Node):
    premature_birth: Optional[str] = None
    fetal_findings: List[str] = field(default_factory=list)
    fetal_findings_details: List[str] = field(default_factory=list)


@dataclass
class PatientPerinatalContextEdge(Relationship):
    start_node: Patient = None
    end_node: PerinatalContext = None


@dataclass
class PerinatalContextPhenotypeEdge(Relationship):
    start_node: PerinatalContext = None
    end_node: Phenotype = None
