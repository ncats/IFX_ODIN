from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from src.models.node import Node


@dataclass
class HarmonizomeGeneAttributeType(Node):
    legacy_id: Optional[int] = None
    name: Optional[str] = None
    association: Optional[str] = None
    description: Optional[str] = None
    resource_group: Optional[str] = None
    measurement: Optional[str] = None
    attribute_group: Optional[str] = None
    attribute_type: Optional[str] = None
    pubmed_ids: Optional[str] = None
    url: Optional[str] = None


@dataclass
class HarmonizomeHgramCDF(Node):
    legacy_protein_id: Optional[int] = None
    legacy_uniprot: Optional[str] = None
    legacy_geneid: Optional[int] = None
    legacy_symbol: Optional[str] = None
    type: Optional[str] = None
    attr_count: Optional[int] = None
    attr_cdf: Optional[Decimal] = None
