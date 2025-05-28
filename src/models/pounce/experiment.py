from dataclasses import dataclass
from datetime import date as date_class
from typing import Optional

from src.core.decorators import facets
from src.models.node import Node, Relationship
from src.models.pounce.investigator import InvestigatorRelationship
from src.models.pounce.platform import Platform


@dataclass
@facets(category_fields=['type', 'category'])
class Experiment(Node):
    name: str = None
    type: str = None
    description: str = None
    design: Optional[str] = None
    category: Optional[str] = None
    run_date: Optional[date_class] = None


@dataclass
class ExperimentInvestigatorRelationship(InvestigatorRelationship):
    start_node: Experiment


@dataclass
class ExperimentPlatformRelationship(Relationship):
    start_node: Experiment
    end_node: Platform