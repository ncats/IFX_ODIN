from dataclasses import asdict, dataclass, field
from typing import List, Optional

from src.core.decorators import facets, search
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
@facets(category_fields=["source"])
@search(text_fields=["source", "name", "description"])
class ExternalLinkProvider(Node):
    source: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    link_count: Optional[int] = None


@dataclass
class ExternalLinkDetail:
    url: str
    source_id: str
    source_id_type: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
@facets(category_fields=["source"])
@search(text_fields=["source", "url", "source_id"])
class ProteinExternalLinkEdge(Relationship):
    start_node: Protein = None
    end_node: ExternalLinkProvider = None
    source: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[str] = None
    source_id_type: Optional[str] = None
    details: List[ExternalLinkDetail] = field(default_factory=list)
