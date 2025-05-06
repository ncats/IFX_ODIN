from dataclasses import dataclass
from typing import Dict

from src.models.gene import Gene, Audited
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class TranscriptLocation:
    start: int = None
    end: int = None
    length: int = None

    def to_dict(self) -> Dict[str, int]:
        ret_dict = {}
        if self.start is not None:
            ret_dict['bp_start'] = self.start
            ret_dict['bp_end'] = self.end
            ret_dict['transcript_length'] = self.length
        return ret_dict

@dataclass
class Transcript(Audited, Node):
    location: TranscriptLocation = None
    ensembl_version: str = None
    support_level: str = None
    is_canonical: bool = None
    MANE_select: str = None
    status: str = None
    Ensembl_Transcript_ID_Provenance: str = None
    RefSeq_Provenance: str = None

@dataclass
class GeneTranscriptRelationship(Relationship, Audited):
    start_node: Gene = None
    end_node: Transcript = None

@dataclass
class TranscriptProteinRelationship(Relationship, Audited):
    start_node: Transcript = None
    end_node: Protein = None

@dataclass
class GeneProteinRelationship(Relationship, Audited):
    start_node: Gene = None
    end_node: Protein = None

@dataclass
class IsoformProteinRelationship(Relationship, Audited):
    start_node: Protein = None
    end_node: Protein = None

