from dataclasses import dataclass, field
from typing import List, Optional, Union

from src.models.node import Node, Relationship

@dataclass
class QueryResult:
    query: Optional[str] = None

@dataclass
class FacetResult:
    value: Optional[str]
    count: int

@dataclass
class FacetQueryResult(QueryResult):
    name: str = None
    facet_values: List[FacetResult] = field(default_factory=list)

@dataclass
class FilterOption:
    field: str
    allowed_values: List[Union[str, bool, int, float]]

@dataclass
class ListFilterSettings:
    settings: List[FilterOption]

    def to_dict(self):
        return {setting.field: setting.allowed_values for setting in self.settings}

    @staticmethod
    def merge(one: "ListFilterSettings", other: "ListFilterSettings") -> "ListFilterSettings":
        if other is None:
            return one
        if one is None:
            return other

        return ListFilterSettings(settings=one.settings + other.settings)

@dataclass
class LinkedListFilterSettings:
    node_filter: Optional[ListFilterSettings] = None
    edge_filter: Optional[ListFilterSettings] = None

    @staticmethod
    def merge(one: "LinkedListFilterSettings", other: "LinkedListFilterSettings") -> "LinkedListFilterSettings":
        if other is None:
            return one
        if one is None:
            return other
        return LinkedListFilterSettings(
            node_filter=ListFilterSettings.merge(one.node_filter, other.node_filter),
            edge_filter=ListFilterSettings.merge(one.edge_filter, other.edge_filter)
        )


@dataclass
class LinkDetails:
    edge: Relationship
    node: Node

@dataclass
class LinkedListQueryResult(QueryResult):
    count: Optional[int] = None
    list: Optional[List[LinkDetails]] = field(default_factory=list)

@dataclass
class ListQueryContext:
    source_data_model: str
    filter: Optional[ListFilterSettings] = None

@dataclass
class LinkedListQueryContext:
    source_data_model: str
    source_id: Optional[str]
    dest_data_model: str
    dest_id: Optional[str]
    edge_model: str
    filter: Optional[LinkedListFilterSettings] = None

@dataclass
class ListQueryResult(QueryResult):
    list: List[Node] = field(default_factory=list)
    facets: FacetQueryResult = None

@dataclass
class CountQueryResult(QueryResult):
    count: int = None

@dataclass
class DetailsQueryResult(QueryResult):
    details: Optional[Node] = None

@dataclass
class ResolveResult(QueryResult):
    match: Optional[Node] = None
    other_matches: Optional[List[Node]] = field(default_factory=list)

