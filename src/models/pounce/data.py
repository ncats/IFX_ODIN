from dataclasses import dataclass, field
from typing import List

from src.models.disease import Disease
from src.models.node import Node, Relationship
from src.models.pounce.experiment import Experiment

@dataclass
class Sample(Node):
    name: str = None
    description: str = None

@dataclass
class Measurement(Node):
    count: int = None
    tpm: float = None

@dataclass
class Compound(Node):
    name: str = None
    chemical_formula: str = None
    smiles: str = None
    inchi: str = None

@dataclass
class SampleMeasurementRelationship(Relationship):
    start_node: Sample
    end_node: Measurement

@dataclass
class MeasurementAnalyteRelationship(Relationship):
    start_node: Measurement
    end_node: Node


@dataclass
class Factor(Node):
    name: str = None
    type: str = None

@dataclass
class Treatment(Factor):
    pass

@dataclass
class Protocol(Factor):
    biospecimen_preparation: str = None
    extraction: str = None
    data_acquisition: str = None


@dataclass
class Biospecimen(Factor):
    organism: List[str] = field(default_factory=list)
    part: str = None
    cell_line: str = None
    sex: str = None
    comment: str = None
    category: str = None
    age: str = None

@dataclass
class BiospecimenDiseaseRelationship(Relationship):
    start_node = Biospecimen
    end_node = Disease

@dataclass
class SampleFactorRelationship(Relationship):
    start_node: Sample
    end_node: Factor

@dataclass
class ExperimentSampleRelationship(Relationship):
    start_node: Experiment
    end_node: Sample



