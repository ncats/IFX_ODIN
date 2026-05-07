from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.core.decorators import facets, search
from src.models.node import Node


@dataclass
@facets(category_fields=['form_type', 'report_type', 'status', 'anonymous'], numeric_fields=['percentage_completed'])
class CaseReport(Node):
    form_type: Optional[str] = None
    report_type: Optional[str] = None
    status: Optional[str] = None
    anonymous: Optional[bool] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    percentage_completed: Optional[int] = None
    comment_count: Optional[int] = None
    outcome_computed: Optional[str] = None
    have_adverse_events_old: Optional[bool] = None
    research_prioritizing: Optional[str] = None
