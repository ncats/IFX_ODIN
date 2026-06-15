from typing import List, Optional

import strawberry
from strawberry.scalars import JSON

from src.interfaces.result_types import FilterOption as FilterOptionBase, ListFilterSettings as ListFilterSettingsBase, \
    LinkedListFilterSettings as LinkedListFilterSettingsBase, ListQueryContext as ListQueryContextBase, \
    UpsetQueryContext as UpsetQueryContextBase


@strawberry.input
class FilterOption(FilterOptionBase):
    allowed_values: JSON


@strawberry.input
class ListFilterSettings(ListFilterSettingsBase):
    settings: List[FilterOption]


@strawberry.input
class LinkedListFilterSettings(LinkedListFilterSettingsBase):
    node_filter: Optional[ListFilterSettings] = None
    edge_filter: Optional[ListFilterSettings] = None


@strawberry.input
class ListQueryContext(ListQueryContextBase):
    filter: Optional[ListFilterSettings] = None

@strawberry.input
class UpsetQueryContext(UpsetQueryContextBase):
    values: Optional[List[str]] = None
