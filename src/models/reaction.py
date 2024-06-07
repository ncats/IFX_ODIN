from dataclasses import dataclass
from src.use_cases.load_csvs_into_neo4j import SimpleEnum


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
class ReactionClass:
    id: str
    level: int = None
    name: str = None


@dataclass
class Reaction:
    id: str
    source_id: str = None
    is_transport: bool = None
    direction: ReactionDirection = None
    label: str = None
    equation: str = None
    html_equation: str = None


@dataclass
class ReactionClassRelationship:
    reaction: Reaction
    ec_class: ReactionClass

@dataclass
class ReactionClassParentRelationship:
    reaction_class: ReactionClass
    parent_class: ReactionClass

@dataclass
class ReactionReactionClassRelationship:
    reaction: Reaction
    reaction_class: ReactionClass
