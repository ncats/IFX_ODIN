from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List

from src.core.decorators import facets
from src.models.node import Node
from src.models.pounce.category_value import CategoryValue


@dataclass
@facets(category_fields=['race', 'ethnicity', 'sex', 'category'], numeric_fields=['age'])
class Demographics(Node):
    age: int = None
    race: str = None
    ethnicity: str = None
    sex: str = None
    categories: List[CategoryValue] = field(default_factory=list)
    phenotype_categories: List[CategoryValue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)