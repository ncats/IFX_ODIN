from dataclasses import dataclass
from datetime import date

from src.models.node import Node, Relationship
from src.models.pounce.investigator import InvestigatorRelationship
from src.models.pounce.platform import Platform


@dataclass
class Experiment(Node):
    name: str = None
    type: str = None
    description: str = None
    design: str = None
    category: str = None
    run_date: date = None


@dataclass
class ExperimentInvestigatorRelationship(InvestigatorRelationship):
    start_node: Experiment


@dataclass
class ExperimentPlatformRelationship(Relationship):
    start_node: Experiment
    end_node: Platform