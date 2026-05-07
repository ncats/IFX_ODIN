from dataclasses import dataclass
from typing import Optional

from src.models.cure.pasc.episode import Episode
from src.models.cure.pasc.phenotype import Phenotype
from src.models.cure.pasc.treatment import Treatment
from src.models.node import Node, Relationship


@dataclass
class Outcome(Node):
    raw_symptom_name: Optional[str] = None
    has_unmatched_phenotype: Optional[bool] = None
    effect: Optional[str] = None
    time_to_effect_amount: Optional[str] = None
    time_to_effect_units: Optional[str] = None


@dataclass
class EpisodeOutcomeEdge(Relationship):
    start_node: Episode = None
    end_node: Outcome = None


@dataclass
class TreatmentOutcomeEdge(Relationship):
    start_node: Treatment = None
    end_node: Outcome = None


@dataclass
class OutcomePhenotypeEdge(Relationship):
    start_node: Outcome = None
    end_node: Phenotype = None
