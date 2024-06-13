from dataclasses import dataclass
from src.models.analyte import Analyte


@dataclass
class Ontology:
    id: str
    commonName: str = None
    HMDBOntologyType: str = None


@dataclass
class AnalyteOntologyRelationship:
    analyte: Analyte
    ontology: Ontology
