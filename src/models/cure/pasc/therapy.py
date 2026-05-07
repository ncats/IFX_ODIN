from dataclasses import dataclass
from typing import Optional

from src.models.cure.pasc.episode import Episode
from src.models.node import Node, Relationship


@dataclass
class Therapy(Node):
    name: Optional[str] = None
    slug: Optional[str] = None


@dataclass
class EpisodeTherapyEdge(Relationship):
    start_node: Episode = None
    end_node: Therapy = None
