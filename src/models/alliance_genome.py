from dataclasses import dataclass
from datetime import datetime
from typing import List

from src.models.disease import Disease
from src.models.gene import Gene
from src.models.node import Relationship


@dataclass
class AllianceGeneDiseaseEdge(Relationship):
    # Temporary Alliance-specific copy of the old flat disease-association shape.
    # Migrate this adapter to the shared src.models.disease.GeneDiseaseEdge details
    # model the next time Alliance disease ingest is touched.
    start_node: Gene = None
    end_node: Disease = None
    types: List[str] = None
    evidence_codes: List[str] = None
    evidence_terms: List[str] = None
    references: List[str] = None
    dates: List[datetime] = None
    sources: List[str] = None
