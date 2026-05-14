from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets
from src.models.cure.pasc.condition import Condition
from src.models.cure.shared.patient import Patient
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["role", "problem_duration", "care_level", "onset_month", "onset_year", 'pregnancy_medications'])
class Episode(Node):
    role: Optional[str] = None
    problem_duration: Optional[str] = None
    additional_info: Optional[str] = None
    drug_additional_details: Optional[str] = None
    onset_month: Optional[str] = None
    onset_year: Optional[str] = None
    care_level: Optional[str] = None
    diagnosis_methods: List[str] = field(default_factory=list)
    pregnancy_medications: Optional[str] = None
    pregnancy_medication_names: List[str] = field(default_factory=list)
    treatment_gestational_age: Optional[str] = None
    pregnancy_outcome: Optional[str] = None
    pregnancy_limited_access_to_treatment: Optional[str] = None
    pregnancy_impacted_ability_to_care_for_newborn: Optional[str] = None


@dataclass
class PersonEpisodeEdge(Relationship):
    start_node: Patient = None
    end_node: Episode = None


@dataclass
@facets(category_fields=['relationship_type'])
class EpisodeConditionEdge(Relationship):
    start_node: Episode = None
    end_node: Condition = None
    relationship_type: Optional[str] = None


@dataclass
@facets(category_fields=['relationship_type'])
class EpisodeEpisodeEdge(Relationship):
    start_node: Episode = None
    end_node: Episode = None
    relationship_type: Optional[str] = None
