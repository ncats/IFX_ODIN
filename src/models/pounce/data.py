from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets
from src.models.disease import Disease
from src.models.node import Node, Relationship
from src.models.pounce.experiment import Experiment


@dataclass
@facets(category_fields=['type'])
class Sample(Node):
    name: str = None
    description: str = None
    type: str = None
    replicate: int = None

@dataclass
class Compound(Node):
    name: str = None
    chemical_formula: str = None
    smiles: str = None
    inchi: str = None

@dataclass
class SampleAnalyteRelationship(Relationship):
    start_node: Sample
    end_node: Node
    count: Optional[int] = None
    raw_data: Optional[float] = None
    stats_ready_data: Optional[float] = None
    tpm: Optional[float] = None

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
@facets(category_fields=['organism', 'part', 'cell_line','category','sex'])
class Biospecimen(Factor):
    organism: List[str] = field(default_factory=list)
    part: Optional[str] = None
    cell_line: Optional[str] = None
    sex: Optional[str] = None
    comment: Optional[str] = None
    category: str = None
    age: Optional[str] = None

@dataclass
class BiospecimenDiseaseRelationship(Relationship):
    start_node = Biospecimen
    end_node = Disease

@dataclass
class SampleFactorRelationship(Relationship):
    start_node: Sample
    end_node: Factor

@dataclass
class SampleBiospecimenRelationship(Relationship):
    start_node: Sample
    end_node: Biospecimen

@dataclass
class ExperimentSampleRelationship(Relationship):
    start_node: Experiment
    end_node: Sample



