from dataclasses import dataclass
from src.models.analyte import Analyte


@dataclass
class Ontology:
    id: str
    commonName: str = ''
    HMDBOntologyType: str = ''


@dataclass
class AnalyteOntologyRelationship:
    analyte: Analyte
    ontology: Ontology
