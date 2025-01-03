from dataclasses import dataclass

from src.models.node import Node, Relationship
from src.models.pounce.data import Sample


@dataclass
class RNAProbe(Node):
    pass

@dataclass
class SampleRNAProbeRelationship(Relationship):
    start_node: Sample = None
    end_node: RNAProbe = None
    value: float = None

@dataclass
class Gene(Node):
    pass

@dataclass
class GeneRNAProbeRelationship(Relationship):
    start_node: Gene
    end_node: RNAProbe

