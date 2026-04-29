from abc import ABC
from datetime import datetime
import os
import platform
import socket
from src.interfaces.metadata import DatabaseMetadata, get_git_metadata
from sqlalchemy import case, func
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import IntegrityError
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter
from src.output_adapters.sql_converters.test import TestSQLOutputConverter
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials
from src.shared.sqlalchemy_tables.pharos_tables_new import (
    AncestryDO,
    AncestryDTO,
    AncestryMONDO,
    AncestryUBERON,
    DataSourceVersion,
    DO,
    DOParent,
    DTO,
    DTOParent,
    ETLRun,
    Mondo,
    MondoParent,
    TinxImportance,
    Uberon,
    UberonParent,
)
from src.shared.sqlalchemy_tables.test_tables import Base as TestBase


class MySQLOutputAdapter(OutputAdapter, MySqlAdapter, ABC):
    database_name: str
    truncate_tables: bool
    output_converter: SQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool = True):
        self.database_name = database_name
        self.truncate_tables = truncate_tables
        OutputAdapter.__init__(self)
        MySqlAdapter.__init__(self, credentials)
        self.update_database(database_name)

    @staticmethod
    def _serialize_rows(converted_objects):
        raw_rows = []
        for obj in converted_objects:
            mapper = sqlalchemy_inspect(type(obj))
            attr_to_column = {
                attr.key: attr.columns[0].name
                for attr in mapper.column_attrs
                if len(attr.columns) == 1
            }
            raw_rows.append({
                attr_to_column.get(k, k): v
                for k, v in obj.__dict__.items()
                if k != '_sa_instance_state'
            })
        all_keys = set().union(*(row.keys() for row in raw_rows))
        return [{key: row.get(key) for key in all_keys} for row in raw_rows]

    @staticmethod
    def _is_fk_integrity_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return isinstance(exc, IntegrityError) and (
            "foreign key constraint fails" in message
            or "cannot add or update a child row" in message
        )

    def _diagnose_fk_batch_failure(self, table_class, rows):
        stmt = mysql_insert(table_class.__table__)
        print(f"Batch insert failed for {table_class.__name__}; retrying row-by-row to isolate FK issue")
        bad_rows = []

        for index, row in enumerate(rows, start=1):
            temp_session = self.get_session()
            try:
                temp_session.execute(stmt, [row])
                temp_session.flush()
            except Exception as row_exc:
                temp_session.rollback()
                bad_rows.append((index, row, row_exc))
                print(f"Bad row {index} for {table_class.__name__}: {row}")
                print(f"Row error: {row_exc}")
                break
            finally:
                temp_session.rollback()
                temp_session.close()

        if bad_rows:
            index, row, row_exc = bad_rows[0]
            raise RuntimeError(
                f"Foreign key insert failed for {table_class.__name__} at row {index}: {row}"
            ) from row_exc
        raise RuntimeError(
            f"Foreign key insert failed for {table_class.__name__}, but row-by-row replay did not isolate a bad row"
        )

    def store(self, objects, single_source=False) -> bool:
        if not isinstance(objects, list):
            objects = [objects]

        object_groups = self.sort_and_convert_objects(objects, keep_nested_objects=True)
        session = self.get_session()

        try:
            for obj_list, labels, is_relationship, start_labels, end_labels, obj_cls in object_groups.values():
                converters = self.output_converter.get_object_converters(obj_cls)
                if converters is None:
                    continue

                if not isinstance(converters, list):
                    converters = [converters]

                for converter in converters:
                    start_time = datetime.now()
                    converted_objects = []
                    for obj in obj_list:
                        result = converter(obj)
                        if isinstance(result, list):
                            converted_objects.extend(result)
                        elif result is not None:
                            converted_objects.append(result)

                    if not converted_objects:
                        continue

                    table_class = converted_objects[0].__class__
                    print(f"Inserting {len(converted_objects)} objects of type {table_class.__name__}")
                    rows = self._serialize_rows(converted_objects)
                    stmt = mysql_insert(table_class.__table__)
                    if table_class is TinxImportance:
                        stmt = stmt.on_duplicate_key_update(
                            score=func.greatest(table_class.score, stmt.inserted.score),
                            doid=case(
                                (stmt.inserted.score > table_class.score, stmt.inserted.doid),
                                else_=table_class.doid,
                            ),
                        )
                    try:
                        session.execute(stmt, rows)
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        if self._is_fk_integrity_error(e):
                            self._diagnose_fk_batch_failure(table_class, rows)
                        raise
                    duration = (datetime.now() - start_time).total_seconds()
                    print(f"Processed {len(converted_objects)} objects in {duration:.2f} seconds.")

            return True

        except Exception as e:
            session.rollback()
            print("Error during insert:", e)
            raise

        finally:
            session.close()

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        effective_truncate = self.truncate_tables if truncate_tables is None else truncate_tables
        self.recreate_mysql_db(self.database_name, effective_truncate)
        return True


class TestOutputAdapter(MySQLOutputAdapter):
    output_converter: TestSQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str):
        MySQLOutputAdapter.__init__(self, credentials, database_name)
        self.output_converter = TestSQLOutputConverter(sql_base=TestBase)

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        super().create_or_truncate_datastore(truncate_tables=truncate_tables)
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        return True


class TCRDOutputAdapter(MySQLOutputAdapter):
    output_converter = TCRDOutputConverter

    def __init__(
        self,
        credentials: DBCredentials,
        database_name: str,
        truncate_tables: bool,
        source_graph_credentials: DBCredentials | dict | None = None,
        source_graph_database: str | None = None,
    ):
        MySQLOutputAdapter.__init__(self, credentials, database_name, truncate_tables=truncate_tables)
        self.output_converter = TCRDOutputConverter()
        self.source_graph_credentials = self._coerce_db_credentials(source_graph_credentials)
        self.source_graph_database = source_graph_database

    def do_pre_processing(self) -> None:
        session = self.get_session()
        try:
            self.output_converter.preload_id_mappings(session)
        finally:
            session.close()

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        super().create_or_truncate_datastore(truncate_tables=truncate_tables)
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        self.output_converter.preload_id_mappings(self.get_session())
        return True

    @staticmethod
    def _build_transitive_closure(node_ids, direct_edges):
        parent_map = {node_id: set() for node_id in node_ids}
        for child_id, parent_id in direct_edges:
            if child_id in parent_map and parent_id in parent_map:
                parent_map[child_id].add(parent_id)

        closure = set()
        for node_id in node_ids:
            closure.add((node_id, node_id))
            stack = list(parent_map.get(node_id, ()))
            visited = set()
            while stack:
                ancestor_id = stack.pop()
                if ancestor_id in visited:
                    continue
                visited.add(ancestor_id)
                closure.add((node_id, ancestor_id))
                stack.extend(parent_map.get(ancestor_id, ()))
        return sorted(closure)

    @staticmethod
    def _coerce_db_credentials(raw_credentials):
        if raw_credentials is None:
            return None
        if isinstance(raw_credentials, DBCredentials):
            return raw_credentials
        if isinstance(raw_credentials, dict):
            return DBCredentials.from_yaml(raw_credentials)
        raise TypeError(f"Unsupported credential type: {type(raw_credentials)}")

    @staticmethod
    def _build_data_source_version_rows(database_metadata: DatabaseMetadata):
        row_map = {}
        for collection in database_metadata.collections:
            for source in collection.sources:
                key = (
                    source.name,
                    source.version,
                    source.version_date,
                    source.download_date,
                )
                if key not in row_map:
                    row_map[key] = {
                        "data_source": source.name,
                        "version": source.version,
                        "version_date": source.version_date,
                        "download_date": source.download_date,
                        "collections": set(),
                    }
                row_map[key]["collections"].add(collection.name)

        rows = []
        for key in sorted(
            row_map,
            key=lambda item: (
                item[0],
                item[1] or "",
                item[2].isoformat() if item[2] else "",
                item[3].isoformat() if item[3] else "",
            ),
        ):
            row = row_map[key]
            rows.append({
                "data_source": row["data_source"],
                "version": row["version"],
                "version_date": row["version_date"],
                "download_date": row["download_date"],
                "collections": ", ".join(sorted(row["collections"])),
            })
        return rows

    def _load_graph_database_metadata(self) -> DatabaseMetadata:
        if self.source_graph_credentials is None or not self.source_graph_database:
            return DatabaseMetadata(collections=[])

        adapter = ArangoAdapter(self.source_graph_credentials, self.source_graph_database)
        store = adapter.get_metadata_store(False)
        metadata_doc = store.get("database_metadata") or {}
        return DatabaseMetadata.from_dict(metadata_doc.get("value", []))

    def _load_graph_etl_metadata(self) -> dict:
        if self.source_graph_credentials is None or not self.source_graph_database:
            return {}

        adapter = ArangoAdapter(self.source_graph_credentials, self.source_graph_database)
        store = adapter.get_metadata_store(False)
        metadata_doc = store.get("etl_metadata") or {}
        return metadata_doc.get("value", {}) or {}

    @staticmethod
    def _build_etl_run_rows(graph_etl_metadata: dict, mysql_database_name: str):
        rows = []

        if graph_etl_metadata and graph_etl_metadata.get("run_date"):
            graph_git = graph_etl_metadata.get("git_info", {}) or {}
            rows.append({
                "stage": "graph_build",
                "database_name": mysql_database_name,
                "run_date": datetime.fromisoformat(graph_etl_metadata["run_date"]),
                "git_commit": graph_git.get("git_commit"),
                "git_branch": graph_git.get("git_branch"),
                "git_tag": graph_git.get("git_tag"),
                "runner": graph_etl_metadata.get("runner"),
                "hostname": graph_etl_metadata.get("hostname"),
                "platform": graph_etl_metadata.get("platform"),
                "platform_version": graph_etl_metadata.get("platform_version"),
                "python_version": graph_etl_metadata.get("python_version"),
            })

        mysql_git = get_git_metadata()
        rows.append({
            "stage": "graph_to_mysql",
            "database_name": mysql_database_name,
            "run_date": datetime.now(),
            "git_commit": mysql_git.get("git_commit"),
            "git_branch": mysql_git.get("git_branch"),
            "git_tag": mysql_git.get("git_tag"),
            "runner": os.getenv("USER", "unknown"),
            "hostname": socket.gethostname(),
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
        })

        return rows

    def _populate_data_source_version_table(self, session):
        database_metadata = self._load_graph_database_metadata()
        rows = self._build_data_source_version_rows(database_metadata)
        session.query(DataSourceVersion).delete()
        if rows:
            session.bulk_insert_mappings(DataSourceVersion, rows)

    def _populate_etl_run_table(self, session):
        graph_etl_metadata = self._load_graph_etl_metadata()
        rows = self._build_etl_run_rows(graph_etl_metadata, self.database_name)
        session.query(ETLRun).delete()
        if rows:
            session.bulk_insert_mappings(ETLRun, rows)

    def _populate_ancestry_table(self, session, node_cls, parent_cls, ancestry_cls, node_key, parent_key):
        node_ids = [row[0] for row in session.query(node_cls).with_entities(getattr(node_cls, node_key)).all() if row[0]]
        direct_edges = session.query(parent_cls).with_entities(
            getattr(parent_cls, node_key),
            getattr(parent_cls, parent_key),
        ).all()
        closure = self._build_transitive_closure(node_ids, direct_edges)
        session.query(ancestry_cls).delete()
        if closure:
            session.bulk_insert_mappings(
                ancestry_cls,
                [{"oid": oid, "ancestor_id": ancestor_id} for oid, ancestor_id in closure],
            )

    @staticmethod
    def _populate_dto_parent_column(session):
        session.query(DTO).update({DTO.parent_id: None})
        direct_edges = session.query(DTOParent.dtoid, DTOParent.parent_id).all()
        if direct_edges:
            session.bulk_update_mappings(
                DTO,
                [{"dtoid": dtoid, "parent_id": parent_id} for dtoid, parent_id in direct_edges],
            )

    def do_post_processing(self, clean_edges: bool = True) -> None:
        session = self.get_session()
        try:
            self._populate_data_source_version_table(session)
            self._populate_etl_run_table(session)
            self._populate_dto_parent_column(session)
            self._populate_ancestry_table(
                session=session,
                node_cls=DO,
                parent_cls=DOParent,
                ancestry_cls=AncestryDO,
                node_key="doid",
                parent_key="parent_id",
            )
            self._populate_ancestry_table(
                session=session,
                node_cls=DTO,
                parent_cls=DTOParent,
                ancestry_cls=AncestryDTO,
                node_key="dtoid",
                parent_key="parent_id",
            )
            self._populate_ancestry_table(
                session=session,
                node_cls=Mondo,
                parent_cls=MondoParent,
                ancestry_cls=AncestryMONDO,
                node_key="mondoid",
                parent_key="parentid",
            )
            self._populate_ancestry_table(
                session=session,
                node_cls=Uberon,
                parent_cls=UberonParent,
                ancestry_cls=AncestryUBERON,
                node_key="uid",
                parent_key="parent_id",
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
