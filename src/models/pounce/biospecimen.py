from dataclasses import field, dataclass
from typing import List, Optional

from src.core.decorators import facets
from src.models.node import Node
from src.models.pounce.category_value import CategoryValue


@dataclass
@facets(category_fields=['type', 'organism'])
class BioSpecimen(Node):
    original_id: str = None
    type: str = None
    description: Optional[str] = None
    organism: str = None
    organism_category: Optional[CategoryValue] = None
    disease_category: Optional[CategoryValue] = None
    phenotype_category: Optional[CategoryValue] = None
    diseases: Optional[List[str]] = field(default_factory=list)