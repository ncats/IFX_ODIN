from dataclasses import dataclass
from typing import Optional

from src.core.decorators import search, facets
from src.models.cure.pasc.episode import Episode
from src.models.node import Node, Relationship


@dataclass
@search(text_fields=['name'])
class Vaccine(Node):
    name: Optional[str] = None
    slug: Optional[str] = None


@dataclass
@facets(category_fields=['vaccinated_before_infection'])
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
