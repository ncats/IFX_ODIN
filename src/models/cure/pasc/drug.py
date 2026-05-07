from dataclasses import dataclass
from typing import Optional

from src.models.node import Node


@dataclass
class Drug(Node):
    name: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[int] = None
    rxnorm_id: Optional[str] = None
    category: Optional[str] = None
    fda_approved: Optional[bool] = None
