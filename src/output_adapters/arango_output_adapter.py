import hashlib

from arango import ArangoClient
from arango.database import StandardDatabase

from src.interfaces.output_adapter import OutputAdapter
from src.shared.db_credentials import DBCredentials
from src.shared.record_merger import RecordMerger, FieldConflictBehavior


class ArangoOutputAdapter(OutputAdapter):
    credentials: DBCredentials
    database_name: str
    client: ArangoClient = None
    db: StandardDatabase = None

    def __init__(self, credentials: DBCredentials, database_name: str):
        self.credentials = credentials
        self.database_name = database_name
        self.initialize()

    def initialize(self):
        self.client = ArangoClient()

    def get_db(self):
        if self.db is None:
            self.db = self.client.db(self.database_name, username=self.credentials.user,
                                     password=self.credentials.password)
        return self.db

    def get_graph(self):
        db = self.get_db()
        if not db.has_graph("graph"):
            db.create_graph("graph")
        return db.graph("graph")

    def store(self, objects) -> bool:
        def generate_edge_key(from_node, to_node, edge_type):
            key_base = f"{edge_type}::{from_node}::{to_node}"
            return hashlib.md5(key_base.encode()).hexdigest()


        merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

        if not isinstance(objects, list):
            objects = [objects]
        db = self.get_db()
        graph = self.get_graph()
        object_groups = self.sort_and_convert_objects(objects, convert_dates=True)
        for obj_list, labels, is_relationship, start_labels, end_labels in object_groups.values():
            label = labels[0]

            if is_relationship:
                if not graph.has_edge_collection(label):
                    edge_collection = graph.create_edge_definition(label, start_labels, end_labels)
                else:
                    edge_collection = graph.edge_collection(label)

                keys = [generate_edge_key(obj['start_id'], obj['end_id'], label) for obj in obj_list]

                existing_edges = edge_collection.get_many(keys)
                existing_record_map = {
                    (record['start_id'], record['end_id']): record for record in existing_edges
                }
                merged_records = merger.merge_records(obj_list, existing_record_map, nodes_or_edges='edges')

                edges = []
                for obj in merged_records:
                    edge = {
                        **obj,
                        "_from": f"{start_labels[0]}/{obj['start_id']}",
                        "_to": f"{end_labels[0]}/{obj['end_id']}",
                        "_key": generate_edge_key(obj["start_id"], obj["end_id"], label)
                    }
                    edges.append(edge)

                edge_collection.insert_many(edges, overwrite=True)
            else:
                if not db.has_collection(label):
                    collection = db.create_collection(label)
                else:
                    collection = db.collection(label)

                keys = [obj['id'] for obj in obj_list]
                existing_nodes = collection.get_many(keys)
                existing_record_map = {
                    record['id']: record for record in existing_nodes
                }
                merged_nodes = merger.merge_records(obj_list, existing_record_map, nodes_or_edges='nodes')

                collection.insert_many(
                    [{**obj, "_key": obj["id"]} for obj in merged_nodes],
                    overwrite=True
                )

        return True

    def create_or_truncate_datastore(self) -> bool:
        sys_db = self.client.db('_system', username=self.credentials.user,
                            password=self.credentials.password)

        if sys_db.has_database(self.database_name):
            sys_db.delete_database(self.database_name)
        sys_db.create_database(self.database_name)

        return True