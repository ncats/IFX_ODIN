from dataclasses import dataclass, field
from typing import List, Optional

from src.models.node import Node, Relationship


@dataclass
class QueryResult:
    query: str

@dataclass
class ListQueryResult(QueryResult):
    list: List[Node]

@dataclass
class LinkDetails:
    edge: Relationship
    node: Node

@dataclass
class LinkedListQueryResult(QueryResult):
    list: List[LinkDetails]

@dataclass
class CountQueryResult(QueryResult):
    count: int

@dataclass
class DetailsQueryResult(QueryResult):
    details: Optional[Node]

@dataclass
class ResolveResult(QueryResult):
    match: Optional[Node]
    other_matches: Optional[List[Node]] = field(default_factory=list)

@dataclass
class FacetResult:
    value: str
    count: int


@dataclass
class FacetQueryResult(QueryResult):
    facet_values: List[FacetResult]
