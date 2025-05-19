from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from src.core.decorators import facets
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
    location: Optional[str] = None
    chromosome: Optional[int] = None
    strand: Optional[Strand] = None

    def to_dict(self) -> Dict[str, str]:
        ret_dict = {}
        if self.location is not None:
            ret_dict['location'] = self.location
            ret_dict['chromosome'] = self.chromosome
        if self.strand is not None:
            ret_dict['chromosome_strand'] = self.strand.value
        return ret_dict

    @classmethod
    def from_dict(cls, data: dict):
        if data is None:
            return None
        return GeneticLocation(location=data.get('location'), chromosome=data.get('chromosome'), strand=Strand.parse(data.get('chromosome_strand')))


@dataclass
class Audited:
    created: Optional[datetime] = None
    updated: Optional[datetime] = None


@dataclass
@facets(category_fields=["symbol"])
class Gene(Audited, Node):
    location: Optional[GeneticLocation] = None
    pubmed_ids: Optional[List[int]] = None
    mapping_ratio: Optional[float] = None
    symbol: Optional[str] = None
    Name_Provenance: Optional[str] = None
    Location_Provenance: Optional[str] = None
    Ensembl_ID_Provenance: Optional[str] = None
    NCBI_ID_Provenance: Optional[str] = None
    HGNC_ID_Provenance: Optional[str] = None
    Symbol_Provenance: Optional[str] = None

