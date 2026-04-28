import dataclasses
import hashlib
import os
import platform
import socket
import time
from datetime import datetime, date, timezone
from enum import Enum
from typing import Type, List, get_origin, get_args, Union

from src.core.decorators import collect_facets, collect_indexed_fields, collect_search_fields
from src.interfaces.metadata import DatabaseMetadata, CollectionMetadata, get_git_metadata
from src.interfaces.output_adapter import OutputAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.shared.arango_adapter import ArangoAdapter
from src.shared.record_merger import RecordMerger, FieldConflictBehavior

from src.shared.db_credentials import DBCredentials

class ArangoOutputAdapter(OutputAdapter, ArangoAdapter):

    def __init__(self, credentials, database_name, minio_credentials=None):
        self._collection_schemas = {}
        self._graph_views = []
        self._graph_view_source_yaml = None
        self.minio_creds = DBCredentials(**minio_credentials) if minio_credentials else None
        super().__init__(credentials=credentials, database_name=database_name)

    def set_graph_views_metadata(self, graph_views=None, source_yaml=None):
        self._graph_views = graph_views or []
        self._graph_view_source_yaml = source_yaml

    @staticmethod
    def _introspect_dataclass(cls) -> dict:
        """Extract a schema dict from a dataclass by inspecting its fields and type hints."""
        skip_fields = {'labels', 'start_node', 'end_node'}
        result = {}

        for f in dataclasses.fields(cls):
            if f.name.startswith('_') or f.name in skip_fields:
                continue
            result[f.name] = ArangoOutputAdapter._type_hint_to_schema(f.type)

        return result

    @staticmethod
    def _type_hint_to_schema(type_hint) -> Union[str, dict]:
        """Convert a Python type hint to a simple schema descriptor."""
        # Unwrap Optional[X] → X
        origin = get_origin(type_hint)
        if origin is Union:
            args = [a for a in get_args(type_hint) if a is not type(None)]
            if len(args) == 1:
                type_hint = args[0]
                origin = get_origin(type_hint)

        # Handle string annotations (forward references)
        if isinstance(type_hint, str):
            return "str"

        # Primitives
        if type_hint is str:
            return "str"
        if type_hint is int:
            return "int"
        if type_hint is float:
            return "float"
        if type_hint is bool:
            return "bool"
        if type_hint is date:
            return "date"
        if type_hint is datetime:
            return "datetime"

        # Enums → "str" (LabeledIntEnum stores its .label, a string, in Arango)
        if isinstance(type_hint, type) and issubclass(type_hint, Enum):
            return "str"

        # List[X]
        if origin is list:
            item_args = get_args(type_hint)
            if item_args:
                item_type = item_args[0]
                # List of dataclasses → nested object
                if isinstance(item_type, type) and dataclasses.is_dataclass(item_type):
                    return {
                        "type": "list",
                        "item_type": "object",
                        "fields": ArangoOutputAdapter._introspect_dataclass(item_type)
                    }
                # List of LabeledIntEnum → labels stored as strings, but record enum class
                # path so the MySQL converter can build a proper integer lookup table
                if isinstance(item_type, type) and issubclass(item_type, int) and issubclass(item_type, Enum):
                    return {
                        "type": "list",
                        "item_type": "str",
                        "enum": f"{item_type.__module__}.{item_type.__qualname__}",
                    }
                inner = ArangoOutputAdapter._type_hint_to_schema(item_type)
                return {"type": "list", "item_type": inner}
            return {"type": "list", "item_type": "str"}

        # Dict[K, V]
        if origin is dict:
            args = get_args(type_hint)
            key_type = ArangoOutputAdapter._type_hint_to_schema(args[0]) if args else "str"
            val_type = ArangoOutputAdapter._type_hint_to_schema(args[1]) if len(args) > 1 else "str"
            return {"type": "dict", "key_type": key_type, "value_type": val_type}

        # Dataclass objects
        if isinstance(type_hint, type) and dataclasses.is_dataclass(type_hint):
            return {
                "type": "object",
                "fields": ArangoOutputAdapter._introspect_dataclass(type_hint)
            }

        return "str"

    def _ensure_bucket(self, s3_client, bucket: str):
        from botocore.exceptions import ClientError
        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchBucket', '403'):
                s3_client.create_bucket(Bucket=bucket)

    def _handle_dataset_nodes(self, objects):
        """Upload DataFrame from any Dataset or StatsResult nodes to MinIO as Parquet, update file_reference."""
        from src.models.pounce.dataset import Dataset
        from src.models.pounce.stats_result import StatsResult

        if not self.minio_creds:
            return

        import io
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        from botocore.client import Config

        creds = self.minio_creds
        endpoint = creds.url
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=creds.user,
            aws_secret_access_key=creds.password,
            config=Config(
                signature_version="s3v4",
                s3={'addressing_style': 'path'}
            ),
            verify=False,
        )
        self._ensure_bucket(s3, creds.schema)

        for obj in objects:
            if isinstance(obj, (Dataset, StatsResult)) and obj._data_frame is not None:
                safe_id = self.safe_key(obj.id).replace(':', '_')
                key = f"{self.database_name}/parquet/{safe_id}.parquet"

                buf = io.BytesIO()
                table = pa.Table.from_pandas(obj._data_frame)
                pq.write_table(table, buf)
                buf.seek(0)

                s3.put_object(Bucket=creds.schema, Key=key, Body=buf)
                print(f"Uploaded Parquet to MinIO: s3://{creds.schema}/{key} ({obj.row_count} rows x {obj.column_count} cols)")

                obj.file_reference = f"s3://{creds.schema}/{key}"
                obj._data_frame = None

    def _project_id_for_workbook_owner(self, obj) -> str:
        from src.models.pounce.project import Project
        from src.models.pounce.experiment import Experiment
        from src.models.pounce.stats_result import StatsResult

        if isinstance(obj, Project):
            return obj.id
        if isinstance(obj, Experiment):
            return obj.id.rsplit("_", 1)[0]
        if isinstance(obj, StatsResult):
            experiment_id = obj.id.split(":", 1)[0]
            return experiment_id.rsplit("_", 1)[0]
        raise TypeError(f"Unsupported workbook owner type: {type(obj).__name__}")

    def _handle_pounce_workbook_nodes(self, objects):
        from src.models.pounce.project import Project
        from src.models.pounce.experiment import Experiment
        from src.models.pounce.stats_result import StatsResult
        from src.models.pounce.workbook_artifact import WorkbookArtifact

        if not self.minio_creds:
            return

        import boto3
        from botocore.client import Config

        creds = self.minio_creds
        s3 = boto3.client(
            "s3",
            endpoint_url=creds.url,
            aws_access_key_id=creds.user,
            aws_secret_access_key=creds.password,
            config=Config(
                signature_version="s3v4",
                s3={'addressing_style': 'path'}
            ),
            verify=False,
        )
        self._ensure_bucket(s3, creds.schema)

        for obj in objects:
            if not isinstance(obj, (Project, Experiment, StatsResult)):
                continue
            workbook = getattr(obj, "workbook", None)
            if not isinstance(workbook, WorkbookArtifact):
                continue
            local_path = workbook._local_path
            if workbook.file_reference and str(workbook.file_reference).startswith("s3://"):
                continue
            if not local_path:
                continue
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Workbook file not found for {type(obj).__name__} {obj.id}: {local_path}")

            project_id = self._project_id_for_workbook_owner(obj)
            original_name = workbook.original_filename or os.path.basename(local_path)
            key = f"{self.database_name}/workbooks/{self.safe_key(project_id)}/{original_name}"
            with open(local_path, "rb") as handle:
                s3.put_object(Bucket=creds.schema, Key=key, Body=handle)
            workbook.original_filename = original_name
            workbook.file_reference = f"s3://{creds.schema}/{key}"
            workbook._local_path = None

    def create_indexes(self, cls: Type, collection):
        indexed_fields = collect_indexed_fields(cls)
        categories, numerics = collect_facets(cls)

        existing_indexes = collection.indexes()
        existing_fields = {tuple(index['fields']) for index in existing_indexes}

        # Create hash indexes for category fields and additional explicitly-indexed fields.
        for field in sorted(indexed_fields | categories):
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


    def store(self, objects, single_source = False) -> bool:

        def generate_edge_key(from_node, to_node, edge_type):
            edge_hash = hashlib.sha256(f"{from_node}|{to_node}".encode("utf-8")).hexdigest()
            return f"{self.safe_key(edge_type)}__{edge_hash}"

        def get_insert_failures(insert_result):
            failed = []
            if not insert_result:
                return failed
            for result in insert_result:
                if isinstance(result, Exception):
                    failed.append(result)
                elif isinstance(result, dict) and result.get("error"):
                    failed.append(result)
            return failed

        merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

        if not isinstance(objects, list):
            objects = [objects]

        self._handle_dataset_nodes(objects)
        self._handle_pounce_workbook_nodes(objects)

        db = self.get_db()
        graph = self.get_graph()
        object_groups = self.sort_and_convert_objects(objects, convert_dates=True)

        # Collect schema info from each object group
        for obj_list, labels, is_relationship, start_labels, end_labels, obj_cls in object_groups.values():
            label = labels[0]
            indexed_fields = collect_indexed_fields(obj_cls)
            categories, numerics = collect_facets(obj_cls)
            text_fields = collect_search_fields(obj_cls)
            if label not in self._collection_schemas:
                schema_entry = {
                    "fields": self._introspect_dataclass(obj_cls),
                    "index_metadata": {
                        "fields": sorted(indexed_fields),
                    },
                    "facet_metadata": {
                        "category_fields": sorted(categories),
                        "numeric_fields": sorted(numerics),
                    },
                    "search_metadata": {
                        "text_fields": sorted(text_fields),
                    },
                }
                if is_relationship:
                    schema_entry["type"] = "edge"
                    schema_entry["from_collections"] = start_labels
                    schema_entry["to_collections"] = end_labels
                else:
                    schema_entry["type"] = "document"
                self._collection_schemas[label] = schema_entry
            else:
                existing = self._collection_schemas[label]
                existing_index_metadata = existing.setdefault("index_metadata", {
                    "fields": [],
                })
                existing_facet_metadata = existing.setdefault("facet_metadata", {
                    "category_fields": [],
                    "numeric_fields": [],
                })
                existing_search_metadata = existing.setdefault("search_metadata", {
                    "text_fields": [],
                })
                existing_index_metadata["fields"] = sorted(
                    set(existing_index_metadata.get("fields", [])) | indexed_fields
                )
                existing_facet_metadata["category_fields"] = sorted(
                    set(existing_facet_metadata.get("category_fields", [])) | categories
                )
                existing_facet_metadata["numeric_fields"] = sorted(
                    set(existing_facet_metadata.get("numeric_fields", [])) | numerics
                )
                existing_search_metadata["text_fields"] = sorted(
                    set(existing_search_metadata.get("text_fields", [])) | text_fields
                )
            if is_relationship and label in self._collection_schemas:
                # Merge from/to collections for edges seen from multiple sources
                existing = self._collection_schemas[label]
                existing["from_collections"] = list(set(existing.get("from_collections", []) + start_labels))
                existing["to_collections"] = list(set(existing.get("to_collections", []) + end_labels))

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

                existing_record_map = {}
                if not single_source:
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

                insert_result = edge_collection.insert_many(edges, overwrite=True)
                failed = get_insert_failures(insert_result)
                if failed:
                    print(f"edge insert failures for {label}:")
                    for failure in failed[:10]:
                        print(failure)
                    raise Exception(f"failed to insert {len(failed)} edge records into {label}")
            else:
                collection, existing_nodes = self.get_existing_nodes(db, label, obj_list, skip_merge=single_source)

                self.create_indexes(obj_cls, collection)
                existing_record_map = {
                    record['id']: record for record in existing_nodes
                }
                merged_nodes = merger.merge_records(obj_list, existing_record_map, nodes_or_edges='nodes')

                print(merged_nodes[0])
                insert_result = collection.insert_many(
                    [{**obj, "_key": self.safe_key(obj["id"])} for obj in merged_nodes],
                    overwrite=True
                )
                failed = get_insert_failures(insert_result)
                if failed:
                    print(f"node insert failures for {label}:")
                    for failure in failed[:10]:
                        print(failure)
                    raise Exception(f"failed to insert {len(failed)} node records into {label}")

        return True

    def get_existing_nodes(self, db, label, obj_list, skip_merge = False):
        if not db.has_collection(label):
            collection = db.create_collection(label)
        else:
            collection = db.collection(label)
        if skip_merge:
            return collection, []
        keys = [self.safe_key(obj['id']) for obj in obj_list]
        existing_nodes = collection.get_many(keys)
        return collection, existing_nodes

    def _delete_minio_prefix(self, prefix: str) -> None:
        if not self.minio_creds:
            return

        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError

        creds = self.minio_creds
        s3 = boto3.client(
            "s3",
            endpoint_url=creds.url,
            aws_access_key_id=creds.user,
            aws_secret_access_key=creds.password,
            config=Config(
                signature_version="s3v4",
                s3={'addressing_style': 'path'}
            ),
            verify=False,
        )

        try:
            s3.head_bucket(Bucket=creds.schema)
        except ClientError:
            return

        paginator = s3.get_paginator("list_objects_v2")
        total_deleted = 0
        for page in paginator.paginate(Bucket=creds.schema, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue
            objects = [{"Key": item["Key"]} for item in contents]
            for i in range(0, len(objects), 1000):
                chunk = objects[i:i + 1000]
                s3.delete_objects(Bucket=creds.schema, Delete={"Objects": chunk})
                total_deleted += len(chunk)
        if total_deleted:
            print(f"Deleted {total_deleted} MinIO objects under s3://{creds.schema}/{prefix}")

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        sys_db = self.client.db('_system', username=self.credentials.user,
                            password=self.credentials.password)

        effective_truncate = True if truncate_tables is None else truncate_tables

        if sys_db.has_database(self.database_name):
            if effective_truncate:
                sys_db.delete_database(self.database_name)
                sys_db.create_database(self.database_name)
                self._delete_minio_prefix(f"{self.database_name}/")
        else:
            sys_db.create_database(self.database_name)

        return True

    def _checkpoint_doc_key(self, run_id: str) -> str:
        return f"etl_checkpoint__{self.safe_key(run_id)}"

    def _read_checkpoint_doc(self, run_id: str) -> dict:
        store = self.get_metadata_store(truncate=False)
        return store.get(self._checkpoint_doc_key(run_id)) or {}

    def _write_checkpoint_doc(self, run_id: str, doc: dict) -> None:
        store = self.get_metadata_store(truncate=False)
        payload = {
            "_key": self._checkpoint_doc_key(run_id),
            "type": "etl_checkpoint",
            "run_id": run_id,
            **doc,
            "last_updated": datetime.now().isoformat(),
        }
        store.insert(payload, overwrite=True)

    def _upsert_collection_schemas_doc(self) -> None:
        if not self._collection_schemas:
            return
        store = self.get_metadata_store(truncate=False)
        existing_doc = store.get("collection_schemas") or {}
        merged_collections = {**existing_doc.get("collections", {}), **self._collection_schemas}
        store.insert({
            "_key": "collection_schemas",
            "collections": merged_collections,
        }, overwrite=True)

    def get_completed_adapter_names(self, run_id: str) -> set[str]:
        doc = self._read_checkpoint_doc(run_id)
        adapters = doc.get("adapters", {}) or {}
        return {
            adapter_name for adapter_name, metadata in adapters.items()
            if metadata.get("status") == "completed"
        }

    def reset_run_state(self, run_id: str) -> None:
        store = self.get_metadata_store(truncate=False)
        existing = store.get(self._checkpoint_doc_key(run_id))
        if existing:
            store.delete(existing["_key"])

    def mark_adapter_running(self, run_id: str, adapter_name: str, adapter_position: int | None = None,
                             adapter_total: int | None = None) -> None:
        doc = self._read_checkpoint_doc(run_id)
        adapters = dict(doc.get("adapters", {}) or {})
        previous = dict(adapters.get(adapter_name, {}) or {})
        adapters[adapter_name] = {
            **previous,
            "status": "running",
            "adapter_position": adapter_position,
            "adapter_total": adapter_total,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_checkpoint_doc(run_id, {"adapters": adapters})

    def mark_adapter_completed(self, run_id: str, adapter_name: str, records_written: int = 0,
                               adapter_position: int | None = None, adapter_total: int | None = None) -> None:
        doc = self._read_checkpoint_doc(run_id)
        adapters = dict(doc.get("adapters", {}) or {})
        previous = dict(adapters.get(adapter_name, {}) or {})
        adapters[adapter_name] = {
            **previous,
            "status": "completed",
            "adapter_position": adapter_position,
            "adapter_total": adapter_total,
            "records_written": records_written,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_checkpoint_doc(run_id, {"adapters": adapters})

    def mark_adapter_failed(self, run_id: str, adapter_name: str, error_message: str | None = None,
                            adapter_position: int | None = None, adapter_total: int | None = None) -> None:
        doc = self._read_checkpoint_doc(run_id)
        adapters = dict(doc.get("adapters", {}) or {})
        previous = dict(adapters.get(adapter_name, {}) or {})
        adapters[adapter_name] = {
            **previous,
            "status": "failed",
            "adapter_position": adapter_position,
            "adapter_total": adapter_total,
            "error_message": error_message,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_checkpoint_doc(run_id, {"adapters": adapters})

    def flush_incremental_metadata(self) -> None:
        self._upsert_collection_schemas_doc()


    def get_metadata(self) -> DatabaseMetadata:
        collections: List[CollectionMetadata] = []
        ignore_list = [self.metadata_store_label]

        db = self.get_db()

        for collection in db.collections():
            if collection['system']:
                continue
            name = collection['name']
            if name in ignore_list:
                continue
            count_query = f"""
                    RETURN COUNT(
                        FOR doc IN `{name}`
                        RETURN 1)
                    """

            cursor = db.aql.execute(count_query)
            coll_obj = CollectionMetadata(name=name, total_count=cursor.pop())

            source_count_query = f"""
                    FOR doc IN `{collection['name']}`
                        LET values = UNIQUE(doc.sources || [])
                        FOR item IN values
                            COLLECT value = item WITH COUNT INTO count
                        SORT count DESC
                        RETURN {{ value, count }}
                    """

            cursor = db.aql.execute(source_count_query)
            res = list(cursor)

            for row in res:
                count = row['count']
                source_tsv = row['value']
                dsd = DataSourceDetails.parse_tsv(source_tsv)
                coll_obj.sources.append(dsd)
                coll_obj.marginal_source_counts[dsd.name] = count

            upset_count_query = f"""FOR doc IN `{collection['name']}`
                LET agg = doc.sources
                LET sortedAgg = SORTED(agg)
                LET key = CONCAT_SEPARATOR("|", sortedAgg)
                COLLECT combo = key WITH COUNT INTO count
                SORT count DESC
                RETURN {{
                    combination: combo,
                    count: count
            }}"""

            cursor = db.aql.execute(upset_count_query)
            res = list(cursor)

            for row in res:
                count = row['count']
                combined_source_tsv = row['combination']
                sources: List[str] = [DataSourceDetails.parse_tsv(source_tsv).name for source_tsv in combined_source_tsv.split('|')]
                coll_obj.joint_source_counts['|'.join(sources)] = count

            collections.append(coll_obj)

        return DatabaseMetadata(collections=collections)

    def do_post_processing(self, clean_edges: bool = True) -> None:
        if clean_edges:
            self.clean_up_dangling_edges()

        existing_store = self.get_metadata_store(truncate=False)
        existing_graph_views = {}
        try:
            existing_doc = existing_store.get("collection_schemas")
            if existing_doc:
                self._collection_schemas = {**existing_doc.get("collections", {}), **self._collection_schemas}
        except Exception:
            pass
        try:
            existing_doc = existing_store.get("graph_views")
            if existing_doc:
                existing_graph_views = existing_doc.get("value", {}).get("views", {}) or {}
        except Exception:
            pass

        metadata_store = self.get_metadata_store(truncate=False)

        db_metadata = self.get_metadata()
        metadata_store.insert({
            "_key": "database_metadata",
            "value": db_metadata.to_dict()
        }, overwrite=True)

        etl_metadata = self.get_etl_metadata()
        metadata_store.insert({
            "_key": "etl_metadata",
            "value": etl_metadata
        }, overwrite=True)

        if self._collection_schemas:
            metadata_store.insert({
                "_key": "collection_schemas",
                "collections": self._collection_schemas
            }, overwrite=True)
            print(f"Wrote collection schemas for {len(self._collection_schemas)} collections")

        graph_views = self.get_graph_views_metadata(existing_graph_views)
        if graph_views:
            metadata_store.insert({
                "_key": "graph_views",
                "value": {
                    "views": graph_views
                }
            }, overwrite=True)
            print(f"Wrote graph views metadata for {len(graph_views)} views")

    def get_etl_metadata(self):
        git_info = get_git_metadata()
        etl_metadata = {
            "_key": f"etl_run_{datetime.now().isoformat()}",
            "type": "etl_run",
            "run_date": datetime.now().isoformat(),
            "runner": os.getenv("USER", "unknown"),
            "git_info": git_info,
            "hostname": socket.gethostname(),
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
        }
        return etl_metadata

    def get_graph_views_metadata(self, existing_graph_views=None):
        existing_graph_views = existing_graph_views or {}
        merged_graph_views = dict(existing_graph_views)

        for graph_view in self._graph_views:
            if not graph_view or "id" not in graph_view:
                continue
            view_id = graph_view["id"]
            normalized_graph_view = dict(graph_view)
            if self._graph_view_source_yaml and "defined_in_yaml" not in normalized_graph_view:
                normalized_graph_view["defined_in_yaml"] = self._graph_view_source_yaml
            merged_graph_views[view_id] = normalized_graph_view

        return merged_graph_views


    def clean_up_dangling_edges(self, batch_size: int = 250000):
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
