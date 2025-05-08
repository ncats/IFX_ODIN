from dataclasses import dataclass
from typing import Dict, Optional

from src.models.gene import Gene, Audited
from src.models.node import Node, Relationship, generate_class_from_dict
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

    @classmethod
    def from_dict(cls, data: dict):
        if data is None:
            return None
        return TranscriptLocation(start=data.get('bp_start'), end=data.get('bp_end'), length=data.get('transcript_length'))

@dataclass
class Transcript(Audited, Node):
    location: Optional[TranscriptLocation] = None
    ensembl_version: Optional[str] = None
    support_level: Optional[str] = None
    is_canonical: Optional[bool] = None
    MANE_select: Optional[str] = None
    status: Optional[str] = None
    Ensembl_Transcript_ID_Provenance: Optional[str] = None
    RefSeq_Provenance: Optional[str] = None

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

