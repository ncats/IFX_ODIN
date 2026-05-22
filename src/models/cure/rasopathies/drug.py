from dataclasses import dataclass
from typing import Optional

from src.core.decorators import search
from src.models.node import Node


@dataclass
@search(text_fields=["name", "source_id"])
class Drug(Node):
    name: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[str] = None
