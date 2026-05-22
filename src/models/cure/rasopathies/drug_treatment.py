from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets, search
from src.models.cure.rasopathies.drug import Drug
from src.models.cure.rasopathies.finding import Finding
from src.models.cure.rasopathies.phenotype import Phenotype
from src.models.cure.shared.patient import Patient
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["severity", "current_dose_change", "treatment_on_going"])
@search(text_fields=["additional_drug_info"])
class DrugTreatment(Node):
    source_treatment_index: Optional[int] = None
    initial_dose_amount: Optional[str] = None
    initial_unit_of_measurement: Optional[str] = None
    initial_frequency: Optional[str] = None
    initial_route: Optional[str] = None
    current_dose_amount: Optional[str] = None
    current_unit_of_measurement: Optional[str] = None
    current_frequency: Optional[str] = None
    current_route: Optional[str] = None
    current_dose_change: Optional[str] = None
    duration_amount: Optional[str] = None
    unit_of_measurement_duration: Optional[str] = None
    treatment_begin: Optional[str] = None
    treatment_begin_month: Optional[str] = None
    treatment_end: Optional[str] = None
    treatment_end_month: Optional[str] = None
    treatment_on_going: Optional[str] = None
    severity: List[str] = field(default_factory=list)
    additional_drug_info: Optional[str] = None


@dataclass
class PatientDrugTreatmentEdge(Relationship):
    start_node: Patient = None
    end_node: DrugTreatment = None


@dataclass
class DrugTreatmentDrugEdge(Relationship):
    start_node: DrugTreatment = None
    end_node: Drug = None


@dataclass
@facets(category_fields=["target_role", "outcome"])
@search(text_fields=["outcome_details"])
class TreatmentResponse(Node):
    source_treatment_index: Optional[int] = None
    source_target_index: Optional[int] = None
    target_role: Optional[str] = None
    outcome: Optional[str] = None
    outcome_details: Optional[str] = None
    time_to_improvement: Optional[str] = None


@dataclass
class DrugTreatmentResponseEdge(Relationship):
    start_node: DrugTreatment = None
    end_node: TreatmentResponse = None


@dataclass
class TreatmentResponseFindingEdge(Relationship):
    start_node: TreatmentResponse = None
    end_node: Finding = None


@dataclass
@facets(category_fields=["outcomes"])
class DrugTreatmentAdverseEventEdge(Relationship):
    start_node: DrugTreatment = None
    end_node: Phenotype = None
    source_adverse_event_index: Optional[int] = None
    source_label: Optional[str] = None
    have_adverse_events: Optional[str] = None
    outcomes: List[str] = field(default_factory=list)
