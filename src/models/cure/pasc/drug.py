from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets, search
from src.models.node import Node


@dataclass
@facets(category_fields=['category', 'fda_approved'])
@search(text_fields=['name', 'source_id', 'rxnorm_id'])
class Drug(Node):
    name: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[int] = None
    rxnorm_id: Optional[str] = None
    category: Optional[str] = None
    fda_approved: Optional[bool] = None
