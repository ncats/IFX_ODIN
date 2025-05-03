from dataclasses import dataclass
from typing import List

from src.models.node import Node


@dataclass
class QueryResult:
    query: str


@dataclass
class ListQueryResult(QueryResult):
    list: List[Node]


@dataclass
class CountQueryResult(QueryResult):
    count: int


@dataclass
class DetailsQueryResult(QueryResult):
    details: dict


@dataclass
class FacetResult:
    value: str
    count: int


@dataclass
class FacetQueryResult(QueryResult):
    facet_values: List[FacetResult]
