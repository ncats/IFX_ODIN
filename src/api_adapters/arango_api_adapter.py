from src.interfaces.data_api_adapter import APIAdapter
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


class ArangoAPIAdapter(APIAdapter, ArangoAdapter):

    def __init__(self, credentials: DBCredentials, database_name: str, label: str):
        APIAdapter.__init__(self, label=label)
        ArangoAdapter.__init__(self, credentials, database_name, internal=True)

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

    def _get_filter_constraint_clause(self, filter: dict):
        clauses = []
        for key, value in filter.items():
            if None in value:
                # Remove None for explicit non-null values
                non_null_values = [v for v in value if v is not None]
                if non_null_values:
                    clause = (
                        f"(IS_ARRAY(doc.{key}) ? LENGTH(INTERSECTION(doc.{key}, {non_null_values})) > 0 : doc.{key} IN {non_null_values}) "
                        f"|| doc.{key} == null || !HAS(doc, '{key}')"
                    )
                else:
                    clause = f"(doc.{key} == null || !HAS(doc, '{key}'))"
            else:
                clause = (
                    f"(IS_ARRAY(doc.{key}) ? LENGTH(INTERSECTION(doc.{key}, {value})) > 0 : doc.{key} IN {value})"
                )
            clauses.append(f"({clause})")
        return ' AND '.join(clauses)

    def _get_document_cleanup_clause(self, variable: str = 'doc'):
        return f"""
            UNSET({variable}, ["_key", "_id", "_rev", "_from", "_to"])
            """

    def get_facet_values(self, data_model: str, field: str, filter: dict = None, top: int = 20):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        other_filter = {k: v for k, v in filter.items() if k != field} if filter else None
        query = f"""
        FOR doc IN `{label}`
            {f"FILTER { self._get_filter_constraint_clause(other_filter) }" if other_filter else ""}
            LET values = (IS_ARRAY(doc.{field}) ? UNIQUE(doc.{field}) : [doc.{field}])
            FOR item IN values
                COLLECT value = item WITH COUNT INTO count
                SORT count DESC
                LIMIT {top}
                RETURN {{ value, count }}
            """

        result = self.runQuery(query)
        return list(result)

    def get_count(self, data_model: str, filter: dict = None):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER { self._get_filter_constraint_clause(filter) }" if filter else ""}
                COLLECT AGGREGATE count = COUNT(doc)
                RETURN count
                """
        result = self.runQuery(query)
        return list(result)[0]

    def get_list_query(self, data_model: str, filter: dict = None, top: int = 20, skip: int = 0):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER { self._get_filter_constraint_clause(filter) }" if filter else ""}
                LIMIT {skip}, {top}
                RETURN {self._get_document_cleanup_clause()}
            """
        return query

    def get_list(self, data_model: str, filter: dict = None, top: int = 20, skip: int = 0):
        query = self.get_list_query(data_model, filter, top, skip)
        result = self.runQuery(query)
        return {"results": list(result), "query": query}

    def get_details(self, data_model: str, id: str):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                FILTER doc.id == '{id}'
                RETURN {self._get_document_cleanup_clause()}
            """
        result = self.runQuery(query)
        return list(result)[0] if result else None

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
            id = start_id
            direction = 'OUTBOUND'
        else:
            id = end_id
            direction = 'INBOUND'

        query = f"""
        FOR v, e IN 1..1 {direction} '{node_label}/{id}' `{edge_label}`
            RETURN {{
                "edge": {self._get_document_cleanup_clause('e')}, 
                "node": {self._get_document_cleanup_clause('v')}
                }}
            """
        print(query)
        result = self.runQuery(query)
        return list(result) if result else None
