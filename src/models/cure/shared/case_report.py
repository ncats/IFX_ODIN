from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from src.core.decorators import facets, search
from src.models.node import Node


@dataclass
@facets(category_fields=['form_type', 'report_type', 'status', 'anonymous'], numeric_fields=['percentage_completed'])
class CaseReport(Node):
    form_type: Optional[str] = None
    report_type: Optional[str] = None
    backend_report_type: Optional[str] = None
    case_report_url: Optional[str] = None
    status: Optional[str] = None
    anonymous: Optional[bool] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    percentage_completed: Optional[int] = None
    comment_count: Optional[int] = None
    outcome_computed: Optional[str] = None
    have_adverse_events_old: Optional[bool] = None
    research_prioritizing: Optional[str] = None
    flagged: Optional[bool] = None
    reminder: Optional[bool] = None
    when_reminder: List[str] = field(default_factory=list)
    previously_approved: Optional[bool] = None
