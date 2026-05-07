from dataclasses import dataclass
from typing import Optional

from src.models.cure.pasc.episode import Episode
from src.models.node import Node, Relationship


@dataclass
class Vaccine(Node):
    name: Optional[str] = None
    slug: Optional[str] = None


@dataclass
class VaccinationEvent(Node):
    vaccinated_before_infection: Optional[str] = None
    dose_count_before_infection: Optional[str] = None


@dataclass
class VaccinationEventEpisodeEdge(Relationship):
    start_node: Episode = None
    end_node: VaccinationEvent = None
    relative_time_value: Optional[str] = None
    relative_time_unit: Optional[str] = None


@dataclass
class VaccinationEventVaccineEdge(Relationship):
    start_node: VaccinationEvent = None
    end_node: Vaccine = None
