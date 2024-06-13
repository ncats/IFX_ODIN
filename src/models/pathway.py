from dataclasses import dataclass

from src.models.analyte import Analyte


@dataclass
class Pathway:
    id: str
    source_id: str = None
    type: str = None
    category: str = None
    name: str = None


@dataclass
class AnalytePathwayRelationship:
    analyte: Analyte
    pathway: Pathway
