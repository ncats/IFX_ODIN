from dataclasses import dataclass

from src.models.node import Relationship
from src.models.pounce.experiment import Experiment
from src.models.pounce.project import Project

@dataclass
class ProjectExperimentRelationship(Relationship):
    start_node: Project
    end_node: Experiment
