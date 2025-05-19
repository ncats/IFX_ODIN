from typing import List, Optional

import networkx as nx

from src.interfaces.data_api_adapter import APIAdapter
from src.interfaces.result_types import FacetQueryResult, DetailsQueryResult, \
    ResolveResult, LinkDetails, LinkedListQueryContext, FacetResult, ListQueryContext
from src.models.node import Node
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


class ArangoAPIAdapter(APIAdapter, ArangoAdapter):

    def __init__(self, credentials: DBCredentials, database_name: str, label: str, imports: List[str]):
        APIAdapter.__init__(self, label=label, imports=imports)
        ArangoAdapter.__init__(self, credentials, database_name, internal=True)


    def get_graph_representation(self, unLabel: bool = False) -> nx.DiGraph:
        graph = self.get_graph()
        g = nx.DiGraph()

        collections = self.get_db().collections()
        node_collections = [collection for collection in collections if not collection['name'].startswith("_") and collection['type'] == 'document' and collection['status'] == 'loaded']
        if unLabel:
            for collection in node_collections:
                for cls in self.labeler.get_classes(collection['name']):
                    g.add_node(cls)
        else:
            g.add_nodes_from([collection['name'] for collection in node_collections])

        for edge_def in graph.edge_definitions():
            for from_node in edge_def['from_vertex_collections']:
                for to_node in edge_def['to_vertex_collections']:
                    edge_label = edge_def['edge_collection']
                    if unLabel:
                        for from_class in self.labeler.get_classes(from_node):
                            for to_class in self.labeler.get_classes(to_node):
                                for edge_class in self.labeler.get_classes(edge_label, True):
                                    edge_class_name = edge_class.__name__ if hasattr(edge_class, '__name__') else edge_class
                                    g.add_edge(from_class, to_class, label=edge_class_name)
                    else:
                        g.add_edge(from_node, to_node, label=edge_label)

        return g

    def _list_models(self, nodes_or_edges = 'nodes'):
        collections = self.get_db().collections()
        if nodes_or_edges == 'nodes':
            models = [collection for collection in collections if not collection['name'].startswith("_") and collection['type'] == 'document' and collection['status'] == 'loaded']
        else:
            models = [collection for collection in collections if not collection['name'].startswith("_") and not collection['type'] == 'document' and collection['status'] == 'loaded']
        model_set = set()
        for model in models:
            model_set.update(self.labeler.get_classes(model['name']))
        return list(model_set)

    def list_edges(self):
        return self._list_models('edges')

    def list_nodes(self, include_edges: bool = False):
        return self._list_models('nodes')

    def _get_filter_constraint_clause(self, filter: dict, variable: str = 'doc'):
        clauses = []
        for key, value in filter.items():

            if None in value:
                # Remove None for explicit non-null values
                non_null_values = [v for v in value if v is not None]
                if non_null_values:
                    clause = (
                        f"(IS_ARRAY({variable}.{key}) ? LENGTH(INTERSECTION({variable}.{key}, {non_null_values})) > 0 : {variable}.{key} IN {non_null_values}) "
                        f"|| {variable}.{key} == null || !HAS({variable}, '{key}')"
                    )
                else:
                    clause = f"({variable}.{key} == null || !HAS({variable}, '{key}'))"
            else:
                clause = (
                    f"(IS_ARRAY({variable}.{key}) ? LENGTH(INTERSECTION({variable}.{key}, {value})) > 0 : {variable}.{key} IN {value})"
                )
            clauses.append(f"({clause})")
        return ' AND '.join(clauses)

    def _get_document_cleanup_clause(self, variable: str = 'doc'):
        return f"""
            UNSET({variable}, ["_key", "_id", "_rev", "_from", "_to"])
            """

    def _get_sortby_clause(self, sortby: dict):
        if not sortby:
            return ""
        clauses = [f"doc.{k} {v}" for k, v in sortby.items()]
        return "SORT " + ', '.join(clauses)

    def _get_facet_clause(self, field: str, top: int = 20, variable: str = 'doc' ):
        return f"""
        LET values = (IS_ARRAY({variable}.{field}) ? UNIQUE({variable}.{field}) : [{variable}.{field}])
            FOR item IN values
                COLLECT value = item WITH COUNT INTO count
                SORT count DESC
                LIMIT {top}
                RETURN {{ value, count }}"""


    def get_count(self, context: ListQueryContext = None) -> int:
        label = self.labeler.get_labels_for_class_name(context.source_data_model)[0]
        filter = context.filter.to_dict() if context and context.filter else None
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER { self._get_filter_constraint_clause(filter) }" if filter else ""}
                COLLECT AGGREGATE count = COUNT(doc)
                RETURN count
                """
        result = self.runQuery(query)

        return result[0] if result else 0

    def get_facets(self, context: ListQueryContext, facets: List[str], top: int) -> List[FacetQueryResult]:
        label = self.labeler.get_labels_for_class_name(context.source_data_model)[0]
        filter = context.filter.to_dict() if context and context.filter else None
        full_results = []
        if facets == None or len(facets) == 0:
            facets = self.get_default_facets(context.source_data_model)
        for field in facets:
            other_filter = {k: v for k, v in filter.items() if k != field} if filter else None
            query = f"""
                FOR doc IN `{label}`
                    {f"FILTER { self._get_filter_constraint_clause(other_filter) }" if other_filter else ""}
                    {self._get_facet_clause(field, top)}
                    """
            results = self.runQuery(query)
            fq_result = FacetQueryResult(
                name = field,
                query = query,
                facet_values=[FacetResult(value=row['value'], count=row['count']) for row in results]
            )
            full_results.append(fq_result)
        return full_results

    def get_query(self, context: ListQueryContext, top: int, skip: int) -> str:
        label = self.labeler.get_labels_for_class_name(context.source_data_model)[0]
        filter = context.filter.to_dict() if context and context.filter else None
        query = self.get_list_query(filter, label, skip, top)
        return query

    def get_list(self, context: ListQueryContext, top: int, skip: int) -> List[Node]:
        label = self.labeler.get_labels_for_class_name(context.source_data_model)[0]
        filter = context.filter.to_dict() if context and context.filter else None
        query = self.get_list_query(filter, label, skip, top)
        result = self.runQuery(query)
        return [self.convert_to_class(context.source_data_model, res) for res in result]

    def get_list_query(self, filter, label, skip, top):
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER {self._get_filter_constraint_clause(filter)}" if filter else ""}
                LIMIT {skip}, {top}
                RETURN {self._get_document_cleanup_clause()}
            """
        return query

    def get_linked_list_facets(self, context: LinkedListQueryContext, node_facets: Optional[List[str]], edge_facets: Optional[List[str]]) \
            -> List[FacetQueryResult]:
        full_results = []
        for coll, variable in zip([node_facets, edge_facets], ['v', 'e']):
            if not coll or len(coll) == 0:
                continue
            for field in coll:

                query_model, collection_clause = self.parse_linked_list_context(context, variable, field)
                facet_clause = self._get_facet_clause(field = field, variable = variable)
                query = f"{collection_clause} {facet_clause}"
                results = self.runQuery(query)
                fq_result = FacetQueryResult(
                    name = field,
                    query = query,
                    facet_values=[FacetResult(value=row['value'], count=row['count']) for row in results]
                )
                full_results.append(fq_result)

        return full_results

    def get_linked_list_count(self, context: LinkedListQueryContext) -> int:
        query_model, collection_clause = self.parse_linked_list_context(context)
        count_query = f"""
            RETURN COUNT(
                {collection_clause}
                RETURN 1)
                """
        count = self.runQuery(count_query)
        return count[0]

    def get_linked_list_query(self, context: LinkedListQueryContext, top: int, skip: int) -> str:
        query_model, collection_clause = self.parse_linked_list_context(context)
        list_query = self.get_linked_list_query_str(collection_clause, context, top, skip)
        return list_query

    def get_linked_list_details(self, context: LinkedListQueryContext, top: int, skip: int) -> List[LinkDetails]:
        query_model, collection_clause = self.parse_linked_list_context(context)
        list_query = self.get_linked_list_query_str(collection_clause, context, top, skip)
        results = self.runQuery(list_query)
        result_list = []
        for row in results:
            result_list.append(LinkDetails(
                node=self.convert_to_class(query_model, row['node']),
                edge=self.convert_to_class(context.edge_model, row['edge'])
            ))
        return result_list

    def get_linked_list_query_str(self, collection_clause, context, top: int, skip: int):
        list_query = f"""
            {collection_clause}
                LIMIT {skip}, {top}
                RETURN {{
                    edge: {self._get_document_cleanup_clause('e')},
                    node: {self._get_document_cleanup_clause('v')}
                  }}
        """
        return list_query

    def parse_linked_list_context(self, context, ignore_var: str = None, ignore_field: str = None) -> (str, str):
        source_label = self.labeler.get_labels_for_class_name(context.source_data_model)[0]
        dest_label = self.labeler.get_labels_for_class_name(context.dest_data_model)[0]
        edge_label = self.labeler.get_labels_for_class_name(context.edge_model)[0]
        if context.source_id is not None:
            id = self.safe_key(context.source_id)
            anchor_label = source_label
            query_label = dest_label
            query_model = context.dest_data_model
            direction = 'OUTBOUND'
        else:
            id = self.safe_key(context.dest_id)
            anchor_label = dest_label
            query_label = source_label
            query_model = context.source_data_model
            direction = 'INBOUND'

        node_filter = context.filter.node_filter.to_dict() if context.filter and context.filter.node_filter else None
        edge_filter = context.filter.edge_filter.to_dict() if context.filter and context.filter.edge_filter else None
        if node_filter and ignore_var == 'v':
            node_filter = {k: v for k, v in node_filter.items() if k != ignore_field}
        if edge_filter and ignore_var == 'e':
            edge_filter = {k: v for k, v in edge_filter.items() if k != ignore_field}
        collection_clause = f"""
            FOR v, e IN 1..1 {direction} '{anchor_label}/{id}' GRAPH 'graph'
                OPTIONS {{ edgeCollections: ['{edge_label}'], vertexCollections: ['{query_label}'] }}
                {f"FILTER { self._get_filter_constraint_clause(node_filter, variable='v') } " if node_filter else ""}
                {f"FILTER { self._get_filter_constraint_clause(edge_filter, variable='e') } " if edge_filter else ""}
        """

        return query_model, collection_clause

    def resolve_id(self, data_model: str, id: str, sortby: dict = {}) -> ResolveResult:
        label = self.labeler.get_labels_for_class_name(data_model)[0]

        query = f"""
            FOR doc IN `{label}`
                FILTER '{id}' IN doc.xref
                {self._get_sortby_clause(sortby)}
                LIMIT 11
                RETURN {self._get_document_cleanup_clause()}
            """

        result = self.runQuery(query)
        result_list =  [self.convert_to_class(data_model, res) for res in result]

        return ResolveResult(
            query = query,
            match=result_list[0] if result_list else None,
            other_matches=result_list[1:] if len(result_list) > 1 else None
        )

    def get_details(self, data_model: str, id: str) -> DetailsQueryResult:
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                FILTER doc.id == '{id}'
                LIMIT 1
                RETURN {self._get_document_cleanup_clause()}
            """
        result = self.runQuery(query)
        result_list =  [self.convert_to_class(data_model, res) for res in result]
        return DetailsQueryResult(query = query, details=result_list[0]) \
            if result \
            else DetailsQueryResult(query = query, details=None)

    def get_edge_types(self, data_model: str):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        graph = self.get_graph()
        collections = graph.edge_definitions()

        outgoing_edges = [coll for coll in collections if label in coll['from_vertex_collections']]
        incoming_edges = [coll for coll in collections if label in coll['to_vertex_collections']]

        return {
            "outgoing": outgoing_edges,
            "incoming": incoming_edges
            }

    def get_edge_list(self, data_model: str,  edge_data_model: str, start_id: str = None, end_id: str = None, top: int = 10, skip: int = 0):
        edge_label = self.labeler.get_labels_for_class_name(edge_data_model)[0]
        node_label = self.labeler.get_labels_for_class_name(data_model)[0]

        if start_id is not None:
            id = self.safe_key(start_id)
            direction = 'OUTBOUND'
        else:
            id = self.safe_key(end_id)
            direction = 'INBOUND'

        query = f"""
        FOR v, e IN 1..1 {direction} '{node_label}/{id}' `{edge_label}`
            LIMIT {skip}, {top}
            RETURN {{
                "edge": {self._get_document_cleanup_clause('e')}, 
                "node": {self._get_document_cleanup_clause('v')}
                }}
            """
        result = self.runQuery(query)
        return list(result) if result else None
