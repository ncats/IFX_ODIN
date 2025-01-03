from dataclasses import dataclass

from src.models.node import Node


@dataclass
class Platform(Node):
    name: str = None
    type: str = None
    url: str = None


