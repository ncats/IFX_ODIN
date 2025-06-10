import time
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

    def do_post_processing(self, batch_size: int = 250000) -> None:
        print('cleaning up dangling edges')
        db = self.get_db()
        graph = self.get_graph()

        for edge_collection in graph.edge_definitions():
            collection_name = edge_collection['edge_collection']
            print(f'cleaning up {collection_name}')

            total_deleted = 0
            last_key = ''

            while True:
                start_time = time.time()

                # Get a batch of edges first, then check them
                key_filter = f"FILTER e._key > '{last_key}'" if last_key else ""

                # Step 1: Get edge batch
                cursor = db.aql.execute(f"""
                    FOR e IN `{collection_name}`
                        {key_filter}
                        SORT e._key
                        LIMIT {batch_size}
                        RETURN {{_key: e._key, _from: e._from, _to: e._to}}
                """)

                edges = list(cursor)
                if not edges:
                    break

                last_key = edges[-1]['_key']

                # Step 2: Check which ones are dangling (batch the DOCUMENT calls)
                from_docs = [e['_from'] for e in edges]
                to_docs = [e['_to'] for e in edges]

                # Check existence in batches
                from_check = db.aql.execute(f"""
                    FOR doc_id IN {from_docs}
                        LET exists = DOCUMENT(doc_id) != null
                        RETURN {{id: doc_id, exists: exists}}
                """)

                to_check = db.aql.execute(f"""
                    FOR doc_id IN {to_docs}
                        LET exists = DOCUMENT(doc_id) != null
                        RETURN {{id: doc_id, exists: exists}}
                """)

                from_exists = {item['id']: item['exists'] for item in from_check}
                to_exists = {item['id']: item['exists'] for item in to_check}

                # Find dangling edges
                dangling_keys = []
                for edge in edges:
                    if not from_exists.get(edge['_from'], True) or not to_exists.get(edge['_to'], True):
                        dangling_keys.append(edge['_key'])

                # Step 3: Delete dangling edges by key
                if dangling_keys:
                    db.aql.execute(f"""
                        FOR key IN {dangling_keys}
                            REMOVE key IN `{collection_name}`
                    """)

                deleted_count = len(dangling_keys)
                total_deleted += deleted_count

                print(f"Processed {len(edges)} edges, deleted {deleted_count} dangling, "
                      f"total deleted: {total_deleted}, time: {time.time() - start_time:.1f}s")

                if len(edges) < batch_size:
                    break

            print(f"Completed {collection_name}: {total_deleted} total edges deleted")

