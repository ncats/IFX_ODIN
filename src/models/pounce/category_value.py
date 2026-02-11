from dataclasses import dataclass, asdict
from typing import Dict

from src.models.node import Node


@dataclass
class CategoryValue(Node):
    name: str = None
    value: str = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)