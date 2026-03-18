from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from src.models.gene import Gene
from src.models.protein import Protein
from src.models.node import Node, Relationship


@dataclass
class Disease(Node):
    name: str = None
    type: Optional[str] = None
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
    types: List[str] = None
    evidence_codes: List[str] = None
    evidence_terms: List[str] = None
    references: List[str] = None
    dates: List[datetime] = None
    sources: List[str] = None


@dataclass
class ProteinDiseaseEdge(Relationship):
    start_node: Protein = None
    end_node: Disease = None
    source: str = None
