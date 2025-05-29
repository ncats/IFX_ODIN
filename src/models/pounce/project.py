from dataclasses import dataclass, field
from datetime import date as date_class
from typing import List, Optional

from src.core.decorators import facets
from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship


class ProjectPrivacy(SimpleEnum):
    Private = "Private"
    NCATS = "NCATS"
    Public = "Public"


@dataclass
class ProjectType(Node):
    name: str = None


@dataclass
@facets(
    category_fields=['lab_groups', 'privacy_level', 'keywords'],
)
class Project(Node):
    name: str = None
    description: str = None
    lab_groups: Optional[List[str]] = field(default_factory=list)
    date: Optional[date_class] = None
    privacy_level: Optional[ProjectPrivacy] = None
    keywords: Optional[List[str]] = field(default_factory=list)
    project_sheet: Optional[str] = None
    experiment_sheet: Optional[str] = None


@dataclass
class ProjectTypeRelationship(Relationship):
    start_node: Project
    end_node: ProjectType
