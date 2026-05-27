from dataclasses import dataclass
from typing import Optional

from src.models.node import Node


@dataclass
class WordCount(Node):
    word: Optional[str] = None
    count: Optional[int] = None
