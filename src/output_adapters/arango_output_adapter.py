from typing import Type
from src.core.decorators import collect_facets
from src.interfaces.output_adapter import OutputAdapter
from src.shared.arango_adapter import ArangoAdapter
from src.shared.record_merger import RecordMerger, FieldConflictBehavior


class ArangoOutputAdapter(OutputAdapter, ArangoAdapter):

    def create_indexes(self, cls: Type, collection):
        categories, numerics = collect_facets(cls)

        print(f"Creating indexes for {cls.__name__}")

        existing_indexes = collection.indexes()
        existing_fields = {tuple(index['fields']) for index in existing_indexes}

        # Create hash indexes for category fields
        for field in sorted(categories):
            field_tuple = (field,)
            if field_tuple not in existing_fields:
                print(f"Creating HASH index on: {field}")
                collection.add_hash_index(fields=[field], sparse=True)

        # Create persistent indexes for numeric fields
        for field in sorted(numerics):
            field_tuple = (field,)
            if field_tuple not in existing_fields:
                print(f"Creating PERSISTENT index on: {field}")
                collection.add_persistent_index(fields=[field], sparse=True)


    def store(self, objects) -> bool:

        def generate_edge_key(from_node, to_node, edge_type):
            return f"{self.safe_key(edge_type)}__{self.safe_key(from_node)}__{self.safe_key(to_node)}"

        merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

        if not isinstance(objects, list):
            objects = [objects]
        db = self.get_db()
        graph = self.get_graph()
        object_groups = self.sort_and_convert_objects(objects, convert_dates=True)
        for obj_list, labels, is_relationship, start_labels, end_labels, obj_cls in object_groups.values():
            label = labels[0]
            if is_relationship:
                if not graph.has_edge_collection(label):
                    edge_collection = graph.create_edge_definition(label, start_labels, end_labels)
                else:
                    edge_definition = [definition for definition in graph.edge_definitions() if definition['edge_collection'] == label][0]
                    updated_from = list(set(edge_definition['from_vertex_collections'] + start_labels))
                    updated_to = list(set(edge_definition['to_vertex_collections'] + end_labels))
                    edge_collection = graph.replace_edge_definition(label, updated_from, updated_to)

                self.create_indexes(obj_cls, edge_collection)
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
                        "_from": f"{start_labels[0]}/{self.safe_key(obj['start_id'])}",
                        "_to": f"{end_labels[0]}/{self.safe_key(obj['end_id'])}",
                        "_key": generate_edge_key(obj["start_id"], obj["end_id"], label)
                    }
                    edges.append(edge)

                edge_collection.insert_many(edges, overwrite=True)


            else:
                collection, existing_nodes = self.get_existing_nodes(db, label, obj_list)

                self.create_indexes(obj_cls, collection)
                existing_record_map = {
                    record['id']: record for record in existing_nodes
                }
                merged_nodes = merger.merge_records(obj_list, existing_record_map, nodes_or_edges='nodes')

                print(merged_nodes[0])
                collection.insert_many(
                    [{**obj, "_key": self.safe_key(obj["id"])} for obj in merged_nodes],
                    overwrite=True
                )

        return True

    def get_existing_nodes(self, db, label, obj_list):
        if not db.has_collection(label):
            collection = db.create_collection(label)
        else:
            collection = db.collection(label)
        keys = [self.safe_key(obj['id']) for obj in obj_list]
        existing_nodes = collection.get_many(keys)
        return collection, existing_nodes

    def create_or_truncate_datastore(self) -> bool:
        sys_db = self.client.db('_system', username=self.credentials.user,
                            password=self.credentials.password)

        if sys_db.has_database(self.database_name):
            sys_db.delete_database(self.database_name)
        sys_db.create_database(self.database_name)

        return True

    def do_post_processing(self) -> None:
        print('cleaning up dangling edges')
        db = self.get_db()
        graph = self.get_graph()
        for edge_collection in graph.edge_definitions():
            print(f'cleaning up {edge_collection.name}')
            cursor = db.aql.execute(f""" 
                            FOR e IN `{edge_collection.name}`
                              FILTER !DOCUMENT(e._from) || !DOCUMENT(e._to)
                              REMOVE e IN `{edge_collection.name}`
                        """)
            result = cursor.statistics()
            deleted_count = result.get('modified', 0)
            if deleted_count > 0:
                print(f"Deleted {deleted_count} dangling edges from {edge_collection.name}")
