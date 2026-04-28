from dataclasses import asdict, dataclass, field
from typing import List, Optional

from src.models.disease import Disease
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class GwasAssociationProvenance:
    study_acc: str
    pubmedid: Optional[int] = None
    trait_uri: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class GwasTrait(Node):
    name: Optional[str] = None
    trait_uri: Optional[str] = None


@dataclass
class GwasTraitDiseaseEdge(Relationship):
    start_node: GwasTrait = None
    end_node: Disease = None


@dataclass
class GwasAssociationDetail:
    source: str
    ensg: Optional[str] = None
    n_study: Optional[int] = None
    n_snp: Optional[int] = None
    n_snpw: Optional[float] = None
    geneNtrait: Optional[int] = None
    geneNstudy: Optional[int] = None
    traitNgene: Optional[int] = None
    traitNstudy: Optional[int] = None
    pvalue_mlog_median: Optional[float] = None
    pvalue_mlog_max: Optional[float] = None
    or_median: Optional[float] = None
    n_beta: Optional[int] = None
    study_N_mean: Optional[float] = None
    rcras: Optional[float] = None
    gene_symbol: Optional[str] = None
    gene_tdl: Optional[str] = None
    gene_family: Optional[str] = None
    gene_idg_list: Optional[bool] = None
    gene_name: Optional[str] = None
    meanRank: Optional[float] = None
    meanRankScore: Optional[float] = None
    provenance_details: List[GwasAssociationProvenance] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ProteinGwasTraitEdge(Relationship):
    start_node: Protein = None
    end_node: GwasTrait = None
    details: List[GwasAssociationDetail] = field(default_factory=list)
    disease_ids: List[str] = field(default_factory=list)
