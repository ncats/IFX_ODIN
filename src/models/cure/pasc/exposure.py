from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from src.core.decorators import facets, search
from src.models.cure.pasc.drug import Drug
from src.models.cure.pasc.episode import Episode
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=['adverse_events', 'adverse_event_outcomes'])
@search(text_fields=['long_drug_name'])
class Exposure(Node):
    source_regimen_id: Optional[int] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    is_initial_regimen: Optional[bool] = None
    long_drug_name: Optional[str] = None
    dose_amount: Optional[str] = None
    unit_of_measurement: Optional[str] = None
    frequency: Optional[str] = None
    route: Optional[str] = None
    treatment_begin: Optional[str] = None
    treatment_begin_month: Optional[str] = None
    treatment_end: Optional[str] = None
    treatment_end_month: Optional[str] = None
    treatment_on_going: Optional[str] = None
    duration_amount: Optional[str] = None
    unit_of_measurement_duration: Optional[str] = None
    have_adverse_events: Optional[str] = None
    adverse_events: List[str] = field(default_factory=list)
    adverse_event_outcomes: List[str] = field(default_factory=list)


@dataclass
class EpisodeExposureEdge(Relationship):
    start_node: Episode = None
    end_node: Exposure = None


@dataclass
class ExposureDrugEdge(Relationship):
    start_node: Exposure = None
    end_node: Drug = None
