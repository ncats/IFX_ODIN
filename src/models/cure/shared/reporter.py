from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets
from src.models.cure.shared.case_report import CaseReport
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["reporter_type", "qualification"])
class Reporter(Node):
    reporter_type: Optional[str] = None
    qualification: Optional[str] = None
    is_staff: Optional[bool] = None
    is_superuser: Optional[bool] = None


@dataclass
class CaseReportReporterEdge(Relationship):
    start_node: CaseReport = None
    end_node: Reporter = None
