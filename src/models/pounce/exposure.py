import hashlib
from dataclasses import dataclass, asdict, field
from typing import Optional, List

from src.core.decorators import facets
from src.models.node import Node, Relationship
from src.models.pounce.biosample import Biosample
from src.models.pounce.category_value import CategoryValue


@dataclass
@facets(category_fields=['type'])
class Exposure(Node):
    names: List[str] = field(default_factory=list)
    type: str = None
    category: Optional[CategoryValue] = None
    concentration: Optional[float] = None
    concentration_unit: Optional[str] = None
    duration: Optional[str] = None
    duration_unit: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    growth_media: Optional[str] = None
    condition: Optional[CategoryValue] = None

    def __post_init__(self):
        if not hasattr(self, 'id') or self.id is None or self.id == 'calculate' or self.id == '':
            self.id = self._calculate_id()

    def _calculate_id(self):
        # Create a string representation of all fields except id
        data = asdict(self)
        data.pop('id', None)  # Remove id if it exists
        content = str(sorted(data.items()))
        return hashlib.sha256(content.encode()).hexdigest()

class BiosampleExposureEdge(Relationship):
    start_node: Biosample
    end_node: Exposure