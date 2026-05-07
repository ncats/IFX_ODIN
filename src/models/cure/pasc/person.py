from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets
from src.models.cure.pasc.case_report import CaseReport
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=["sex", "gender", "gender_same_as_sex", "age_group", "ethnicity", "pregnant", "country_treated", "race"])
class Person(Node):
    sex: Optional[str] = None
    gender: Optional[str] = None
    gender_same_as_sex: Optional[str] = None
    age_group: Optional[str] = None
    ethnicity: Optional[str] = None
    pregnant: Optional[str] = None
    country_treated: Optional[str] = None
    race: List[str] = field(default_factory=list)


@dataclass
class CaseReportPersonEdge(Relationship):
    start_node: CaseReport = None
    end_node: Person = None
