from dataclasses import dataclass
from typing import Optional

from src.models.node import Node


@dataclass
class Condition(Node):
    name: Optional[str] = None
    slug: Optional[str] = None
    source_id: Optional[int] = None
