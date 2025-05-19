from dataclasses import dataclass, field
from typing import List

from src.core.decorators import facets
from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship
from src.models.pounce.project import Project


class Role(SimpleEnum):
    Owner = "Owner"
    Collaborator = "Collaborator"
    Contact = "Contact"
    DataGenerator = "DataGenerator"
    Informatician = "Informatician"


@dataclass
@facets(category_fields=['institute', 'branch'])
class Investigator(Node):
    name: str = None
    email: str = None
    institute: List[str] = field(default_factory=list)
    branch: List[str] = field(default_factory=list)


@dataclass
@facets(category_fields=['roles'])
class InvestigatorRelationship(Relationship):
    end_node: Investigator
    roles: List[Role] = field(default_factory=list)

@dataclass
class ProjectInvestigatorRelationship(InvestigatorRelationship):
    start_node: Project