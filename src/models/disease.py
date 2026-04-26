from dataclasses import dataclass, field, asdict
from typing import List, Optional

from src.models.gene import Gene
from src.models.protein import Protein
from src.models.node import Node, Relationship


@dataclass
class Disease(Node):
    name: str = None
    type: Optional[str] = None
    novelty: Optional[List[float]] = None
    mondo_description: Optional[str] = None
    do_description: Optional[str] = None
    uniprot_description: Optional[str] = None
    subsets: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None
    comments: Optional[List[str]] = None


@dataclass
class DiseaseParentEdge(Relationship):
    start_node: Disease = None
    end_node: Disease = None
    source: str = None


@dataclass
class DODiseaseParentEdge(Relationship):
    start_node: Disease = None
    end_node: Disease = None
    source: str = None

@dataclass
class GeneDiseaseEdge(Relationship):
    start_node: Gene = None
    end_node: Disease = None
    details: List["DiseaseAssociationDetail"] = field(default_factory=list)


@dataclass
class DiseaseAssociationDetail:
    source: str
    source_id: Optional[str] = None
    importance: List[float] = field(default_factory=list)
    evidence_terms: List[str] = field(default_factory=list)
    pmids: List[str] = field(default_factory=list)
    evidence_codes: List[str] = field(default_factory=list)
    confidence: Optional[float] = None
    zscore: Optional[float] = None
    url: Optional[str] = None
    drug_name: Optional[str] = None
    snomed_id: Optional[str] = None
    doid: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ProteinDiseaseEdge(Relationship):
    start_node: Protein = None
    end_node: Disease = None
    details: List[DiseaseAssociationDetail] = field(default_factory=list)


@dataclass
class TINXImportanceEdge(Relationship):
    start_node: Protein = None
    end_node: Disease = None
    details: List[DiseaseAssociationDetail] = field(default_factory=list)
