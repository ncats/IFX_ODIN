from src.interfaces.data_api_adapter import APIAdapter
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


class ArangoAPIAdapter(APIAdapter, ArangoAdapter):

    def __init__(self, credentials: DBCredentials, database_name: str, label: str):
        APIAdapter.__init__(self, label=label)
        ArangoAdapter.__init__(self, credentials, database_name, internal=True)

    def list_data_models(self):
        collections = self.get_db().collections()

        models = [collection for collection in collections if not collection['name'].startswith("_") and collection['type'] == 'document' and collection['status'] == 'loaded']

        model_set = set()
        for model in models:
            model_set.update(self.labeler.get_classes(model['name']))
        return list(model_set)

    def get_facet_values(self, data_model: str, field: str, filter: dict = None, top: int = 20):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        other_filter = {k: v for k, v in filter.items() if k != field} if filter else None
        query = f"""
        FOR doc IN `{label}`
            {f"FILTER { ' AND '.join([f'doc.{key} IN {value}' for key, value in other_filter.items()]) }" if other_filter else ""}
            COLLECT facet = doc.{field} WITH COUNT INTO count
            SORT count DESC
            LIMIT {top}
            RETURN {{ facet, count }}
            """
        result = self.runQuery(query)
        return list(result)

    def get_count(self, data_model: str, filter: dict = None):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER { ' AND '.join([f'doc.{key} IN {value}' for key, value in filter.items()]) }" if filter else ""}
                COLLECT AGGREGATE count = COUNT(doc)
                RETURN count
                """
        result = self.runQuery(query)
        return list(result)[0]

    def get_list(self, data_model: str, filter: dict = None, top: int = 20, skip: int = 0):
        label = self.labeler.get_labels_for_class_name(data_model)[0]
        query = f"""
            FOR doc IN `{label}`
                {f"FILTER { ' AND '.join([f'doc.{key} IN {value}' for key, value in filter.items()]) }" if filter else ""}
                LIMIT {skip}, {top}
                RETURN UNSET(doc, ["_key", "_id", "_rev"])
            """
        result = self.runQuery(query)
        return list(result)