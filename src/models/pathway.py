from dataclasses import dataclass

from src.models.analyte import Analyte


@dataclass
class Pathway:
    id: str
    source_id: str = ''
    type: str = ''
    category: str = ''
    name: str = ''


@dataclass
class AnalytePathwayRelationship:
    analyte: Analyte
    pathway: Pathway
