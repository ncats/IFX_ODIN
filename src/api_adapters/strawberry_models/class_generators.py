from typing import Type, List, Optional

import strawberry

from src.interfaces.result_types import LinkedListQueryResult, ResolveResult, DetailsQueryResult, ListQueryResult, \
    LinkDetails, FacetQueryResult as FacetQueryResultBase, FacetResult, DerivedLinkDetails


def make_linked_details_type(details_class_name: str, edge_type: Type, node_type: Type):
    def edge_field(root) -> edge_type:
        return root.edge
    def node_field(root) -> node_type:
        return root.node

    new_class = type(
        details_class_name,
        (LinkDetails,),
        {
            "edge": strawberry.field(resolver = edge_field),
            "node": strawberry.field(resolver = node_field)
        }
    )
    return strawberry.type(new_class)

def make_derived_linked_details_type(details_class_name: str, node_type: Type):
    def node_field(root) -> node_type:
        return root.node

    new_class = type(
        details_class_name,
        (DerivedLinkDetails,),
        {
            "node": strawberry.field(resolver=node_field)
        }
    )
    return strawberry.type(new_class)

def make_networked_list_result_type(query_class_name: str, details_class_name: str, node_type: Type):
    details_class = make_derived_linked_details_type(details_class_name, node_type)
    def list_field(root, top: int = 10, skip: int = 0) -> List[details_class]:
        return root._query_service.get_networked_list_details(root._query_context, top, skip)

    def count_field(root) -> int:
        return root._query_service.get_networked_list_count(root._query_context)

    def query_field(root, top: int = 10, skip: int = 0) -> str:
        return root._query_service.get_networked_list_query(root._query_context, top, skip)

    def facets_field(root, node_facets: Optional[List[str]] = None) -> List[FacetQueryResult]:
        return root._query_service.get_networked_list_facets(root._query_context, node_facets)

    new_class = type(
        query_class_name,
        (LinkedListQueryResult,),
        {
            "query": strawberry.field(resolver=query_field),
            "list": strawberry.field(resolver=list_field),
            "count": strawberry.field(resolver=count_field),
            "facets": strawberry.field(resolver=facets_field)
        }
    )
    return strawberry.type(new_class)

def make_linked_list_result_type(query_class_name: str, details_class_name: str, edge_type: Type, node_type: Type):
    details_class = make_linked_details_type(details_class_name, edge_type, node_type)
    def list_field(root, top: int = 10, skip: int = 0) -> List[details_class]:
        return root._query_service.get_linked_list_details(root._query_context, top, skip)

    def count_field(root) -> int:
        return root._query_service.get_linked_list_count(root._query_context)

    def query_field(root, top: int = 10, skip: int = 0) -> str:
        return root._query_service.get_linked_list_query(root._query_context, top, skip)

    def facets_field(root, node_facets: Optional[List[str]] = None, edge_facets: Optional[List[str]] = None) -> List[FacetQueryResult]:
        return root._query_service.get_linked_list_facets(root._query_context, node_facets, edge_facets)

    new_class = type(
        query_class_name,
        (LinkedListQueryResult,),
        {
            "query": strawberry.field(resolver=query_field),
            "list": strawberry.field(resolver=list_field),
            "count": strawberry.field(resolver=count_field),
            "facets": strawberry.field(resolver=facets_field)
        }
    )
    return strawberry.type(new_class)


def make_resolve_result_type(class_name: str, match_type: Type):
    # Create a new type dynamically
    def match_field(root) -> Optional[match_type]:
        return root.match

    def other_matches_field(root) -> Optional[List[match_type]]:
        return root.other_matches

    new_class = type(
        class_name,
        (ResolveResult,),
        {
            "match": strawberry.field(resolver=match_field),
            "other_matches": strawberry.field(resolver=other_matches_field),
        },
    )
    return strawberry.type(new_class)


def make_details_result_type(class_name: str, match_type: Type):
    def details_field(root) -> Optional[match_type]:
        return root.details

    new_class = type(
        class_name, (DetailsQueryResult,),
        {
            "details": strawberry.field(resolver=details_field)
        }
    )
    return strawberry.type(new_class)


def make_list_result_type(class_name: str, match_type: Type):
    def list_field(root, top: int = 10, skip: int = 0) -> List[match_type]:
        return root._query_service.get_list(root._query_context, top, skip)

    def facets_field(root, facets: Optional[List[str]] = None, top: int = 20) -> List[FacetQueryResult]:
        return root._query_service.get_facets(root._query_context, facets, top)

    def query_field(root, top: int = 10, skip: int = 0) -> str:
        return root._query_service.get_query(root._query_context, top, skip)

    def count_field(root) -> int:
        return root._query_service.get_count(root._query_context)

    new_class = type(
        class_name, (ListQueryResult,),
        {
            "list": strawberry.field(resolver=list_field),
            "facets": strawberry.field(resolver=facets_field),
            "query": strawberry.field(resolver=query_field),
            "count": strawberry.field(resolver=count_field)
        }
    )
    return strawberry.type(new_class)


LinkDetails = strawberry.type(LinkDetails)


@strawberry.type
class FacetQueryResult(FacetQueryResultBase):
    facet_values: List[FacetResult]


FacetResult = strawberry.type(FacetResult)
