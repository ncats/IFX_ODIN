from dataclasses import dataclass, field, asdict
from typing import List, Optional

from src.core.decorators import facets
from src.models.node import Relationship
from src.models.protein import Protein
from src.models.tissue import Tissue


@dataclass
@facets(category_fields=["source", "tissue", "sex"])
class ExpressionDetail:
    source: str
    tissue: Optional[str] = None
    source_id: Optional[str] = None       # gene/protein ID as it appears in the source
    source_tissue_id: Optional[str] = None  # tissue/cell-line ontology ID from source (UBERON, BTO, CLDB, etc.)
    sex: Optional[str] = None             # None = all sexes combined; "male" / "female"
    number_value: Optional[float] = None  # TPM, nTPM, spectral count, confidence score, etc.
    qual_value: Optional[str] = None      # HPA protein: "High" / "Medium" / "Low" / "Not detected"
    expressed: Optional[bool] = None      # HPA RNA, HPM, JensenLab
    source_rank: Optional[float] = None   # normalised rank within source (0.0–1.0)
    evidence: Optional[str] = None        # HPA Protein, HPM
    cell_type: Optional[str] = None       # HPA Protein IHC cell type

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ProteinTissueExpressionEdge(Relationship):
    start_node: Protein
    end_node: Tissue
    details: List[ExpressionDetail] = field(default_factory=list)
