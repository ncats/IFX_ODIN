from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node

class Strand(SimpleEnum):
    plus1 = "+1"
    minus1 = "-1"

    @classmethod
    def parse(cls, input_value: str):
        if input_value is None or input_value == '':
            return None
        int_val = int(float(input_value))
        if int_val > 0:
            return Strand.plus1
        return Strand.minus1

@dataclass
class GeneticLocation:
    location: str = None
    chromosome: int = None
    strand: Strand = None

    def to_dict(self) -> Dict[str, str]:
        ret_dict = {}
        if self.location is not None:
            ret_dict['location'] = self.location
            ret_dict['chromosome'] = self.chromosome
        if self.strand is not None:
            ret_dict['chromosome_strand'] = self.strand.value
        return ret_dict

@dataclass
class Audited:
    created: datetime = None
    updated: datetime = None


@dataclass
class Gene(Audited, Node):
    location: GeneticLocation = None
    pubmed_ids: List[int] = None
    mapping_ratio: float = None

    def __init__(self, **kwargs):
        Node.__init__(self, **kwargs)
