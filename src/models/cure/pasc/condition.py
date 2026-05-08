from dataclasses import dataclass
from typing import Optional

from src.core.decorators import search
from src.models.node import Node


@dataclass
@search(text_fields=['name'])
class Condition(Node):
    name: Optional[str] = None
    slug: Optional[str] = None
    source_id: Optional[int] = None
