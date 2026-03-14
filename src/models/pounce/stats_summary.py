from dataclasses import dataclass
from typing import Optional

from src.models.node import Node
from src.models.pounce.experiment import PlatformType
from src.models.pounce.project import ProjectType


@dataclass
class AnalyteSummary(Node):
    platform_type: Optional[PlatformType] = None
    project_type: Optional[ProjectType] = None
    analyte_count: Optional[int] = None