from dataclasses import dataclass
from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship


class ReactionDirection(SimpleEnum):
    Undirected = "UN"
    LeftToRight = "LR"
    RightToLeft = "RL"
    Bidirectional = "BD"

    @staticmethod
    def parse(value):
        if value == 'UN':
            return ReactionDirection.Undirected
        if value == 'LR':
            return ReactionDirection.LeftToRight
        if value == 'RL':
            return ReactionDirection.RightToLeft
        if value == 'BD':
            return ReactionDirection.Bidirectional
        raise LookupError(f"unknown value for reaction direction: {value}")


@dataclass
class ReactionClass(Node):
    level: int = None
    name: str = None


@dataclass
class Reaction(Node):
    source_id: str = None
    is_transport: bool = None
    direction: ReactionDirection = None
    label: str = None
    equation: str = None
    html_equation: str = None


@dataclass
class ReactionClassEdge(Relationship):
    start_node: Reaction
    end_node: ReactionClass

@dataclass
class ReactionClassParentEdge(Relationship):
    start_node: ReactionClass
    end_node: ReactionClass

@dataclass
class ReactionReactionClassEdge(Relationship):
    start_node: Reaction
    end_node: ReactionClass
