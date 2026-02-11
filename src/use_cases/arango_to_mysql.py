"""
Generic Arango → MySQL converter.

Reads collection_schemas metadata from an ArangoDB metadata_store,
dynamically generates MySQL tables via SQLAlchemy, and copies all data over.
Works with any Arango DB that was built by ArangoOutputAdapter.
"""
import os
import re

from sqlalchemy import MetaData, Table, Column, String, Text, Integer, Float, Boolean, ForeignKey, Index

from src.input_adapters.sql_adapter import MySqlAdapter
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


# Map schema type strings to SQLAlchemy column types
_SQL_TYPE_MAP = {
    "str": Text,
    "int": Integer,
    "float": Float,
    "bool": Boolean,
    "date": Text,       # stored as ISO strings in Arango
    "datetime": Text,   # stored as ISO strings in Arango
}

# Fields from the base Node class that don't need MySQL tables
_SKIP_FIELDS = {"sources", "xref", "provenance"}


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()


def _edge_table_name(schema: dict) -> str:
    """Derive a MySQL table name for an edge collection: 'from_to_to'."""
    from_name = _camel_to_snake(schema["from_collections"][0])
    to_name = _camel_to_snake(schema["to_collections"][0])
    return f"{from_name}_to_{to_name}"


class ArangoToMySqlConverter(ArangoAdapter):
    """Reads from ArangoDB, writes to MySQL using schema metadata."""

    def __init__(self, arango_credentials: DBCredentials, arango_db_name: str,
                 mysql_credentials: DBCredentials, mysql_db_name: str,
                 parquet_dir: str = None):
        super().__init__(credentials=arango_credentials, database_name=arango_db_name)
        self.mysql = MySqlAdapter(mysql_credentials)
        self.mysql_db_name = mysql_db_name
        self.parquet_dir = parquet_dir
        self.sa_metadata = MetaData()

    def convert(self, batch_size: int = 10000):
        """Run the full conversion from Arango to MySQL."""
        schemas = self._read_schemas()
        if not schemas:
            raise RuntimeError("No collection_schemas found in metadata_store")

        self.mysql.recreate_mysql_db(self.mysql_db_name)
        engine = self.mysql.get_engine()

        # Plan data tables — identifies which edge collections are replaced by data table FKs
        data_implicit_edges, data_table_configs = self._plan_data_tables(schemas)

        # Pass 1: create all tables
        document_collections = {}
        edge_collections = {}

        for collection_name, schema in schemas.items():
            if schema["type"] == "document":
                table, child_tables = self._create_document_table(collection_name, schema["fields"])
                document_collections[collection_name] = (table, child_tables, schema)
            elif collection_name not in data_implicit_edges:
                table = self._create_edge_table(collection_name, schema)
                edge_collections[collection_name] = (table, schema)

        data_tables = {}
        for config in data_table_configs:
            table = self._create_data_table(config)
            data_tables[config["table_name"]] = (table, config)

        self.sa_metadata.create_all(engine)

        # Pass 2: copy documents and edges
        for collection_name, (table, child_tables, schema) in document_collections.items():
            self._copy_document_collection(engine, collection_name, table, child_tables, schema, batch_size)

        for collection_name, (table, schema) in edge_collections.items():
            self._copy_edge_collection(engine, collection_name, table, schema, batch_size)

        # Pass 3: melt parquet data into typed data tables
        if data_tables:
            self._melt_parquet_data(engine, data_tables)

        print("Conversion complete.")

    def _read_schemas(self) -> dict:
        """Read collection_schemas from Arango metadata_store."""
        db = self.get_db()
        if not db.has_collection(self.metadata_store_label):
            return {}
        store = db.collection(self.metadata_store_label)
        doc = store.get("collection_schemas")
        if doc is None:
            return {}
        return doc.get("collections", {})

    # --- Data table planning ---

    def _plan_data_tables(self, schemas: dict):
        """Identify data-implicit edges and plan typed data tables.

        For collections with file_reference (Dataset, StatsResult), outgoing edges
        are replaced by foreign keys in data tables. Analyte targets (Gene, Metabolite)
        become row FKs. Sample targets (RunBiosample) become column FKs.

        Returns (data_implicit_edges, data_table_configs).
        """
        edge_schemas = {k: v for k, v in schemas.items() if v["type"] == "edge"}
        file_ref_collections = {
            k for k, v in schemas.items()
            if v["type"] == "document" and "file_reference" in v.get("fields", {})
        }

        data_implicit_edges = set()
        data_table_configs = []

        for parent_coll in file_ref_collections:
            parent_table = _camel_to_snake(parent_coll)
            analyte_targets = []
            sample_target = None

            for edge_name, edge_schema in edge_schemas.items():
                if parent_coll not in edge_schema.get("from_collections", []):
                    continue
                data_implicit_edges.add(edge_name)

                target_coll = edge_schema["to_collections"][0]
                target_table = _camel_to_snake(target_coll)

                if self._is_sample_target(target_coll, edge_schemas, file_ref_collections):
                    sample_target = {"collection": target_coll, "table": target_table, "edge": edge_name}
                else:
                    analyte_targets.append({"collection": target_coll, "table": target_table, "edge": edge_name})

            for analyte in analyte_targets:
                table_name = f"{analyte['table']}_{parent_table}__data"
                data_table_configs.append({
                    "table_name": table_name,
                    "parent_collection": parent_coll,
                    "parent_table": parent_table,
                    "analyte": analyte,
                    "sample": sample_target,
                })

        return data_implicit_edges, data_table_configs

    @staticmethod
    def _is_sample_target(target_coll: str, edge_schemas: dict, file_ref_collections: set) -> bool:
        """A target is a sample type if some non-file-reference collection also points to it.

        e.g. Biosample → RunBiosample exists, so RunBiosample is a sample target.
        Gene/Metabolite are only pointed to by file_reference collections, so they're analyte targets.
        """
        for edge_schema in edge_schemas.values():
            if target_coll not in edge_schema.get("to_collections", []):
                continue
            from_coll = edge_schema["from_collections"][0]
            if from_coll not in file_ref_collections:
                return True
        return False

    # --- Table creation ---

    def _create_document_table(self, name: str, fields: dict) -> tuple:
        """Create a SQLAlchemy Table for a document collection. Returns (table, child_tables_dict)."""
        table_name = _camel_to_snake(name)
        columns = []
        child_tables = {}

        for field_name, field_schema in fields.items():
            if field_name in _SKIP_FIELDS:
                continue
            if isinstance(field_schema, dict):
                field_type = field_schema.get("type")
                if field_type == "list":
                    child_table = self._create_child_table(table_name, field_name, field_schema)
                    child_tables[field_name] = child_table
                    continue
                elif field_type == "object":
                    columns.append(Column(field_name, Text))
                    continue
            col_type = _SQL_TYPE_MAP.get(field_schema, Text)
            if field_name == "id":
                columns.append(Column("id", String(255), primary_key=True))
            else:
                columns.append(Column(field_name, col_type, nullable=True))

        table = Table(table_name, self.sa_metadata, *columns)
        return table, child_tables

    def _create_child_table(self, parent_name: str, field_name: str, field_schema: dict) -> Table:
        """Create a child table for a list field."""
        table_name = f"{parent_name}__{field_name}"
        item_type = field_schema.get("item_type")

        fk = ForeignKey(f"{parent_name}.id")

        if isinstance(item_type, dict) and item_type.get("type") == "object":
            columns = [
                Column("parent_id", String(255), fk, nullable=False),
            ]
            for sub_name, sub_schema in item_type.get("fields", {}).items():
                col_type = _SQL_TYPE_MAP.get(sub_schema, Text) if isinstance(sub_schema, str) else Text
                columns.append(Column(sub_name, col_type, nullable=True))
            columns.append(Index(f"ix_{table_name}_parent", "parent_id"))
        else:
            col_type = _SQL_TYPE_MAP.get(item_type, Text) if isinstance(item_type, str) else Text
            columns = [
                Column("parent_id", String(255), fk, nullable=False),
                Column("value", col_type, nullable=True),
                Index(f"ix_{table_name}_parent", "parent_id"),
            ]

        return Table(table_name, self.sa_metadata, *columns)

    def _create_edge_table(self, name: str, schema: dict) -> Table:
        """Create a SQLAlchemy Table for an edge collection."""
        table_name = _edge_table_name(schema)
        from_table = _camel_to_snake(schema["from_collections"][0])
        to_table = _camel_to_snake(schema["to_collections"][0])
        columns = [
            Column("from_id", String(255), ForeignKey(f"{from_table}.id"), nullable=False),
            Column("to_id", String(255), ForeignKey(f"{to_table}.id"), nullable=False),
        ]

        for field_name, field_schema in schema.get("fields", {}).items():
            if field_name in _SKIP_FIELDS:
                continue
            if isinstance(field_schema, dict):
                continue
            col_type = _SQL_TYPE_MAP.get(field_schema, Text)
            columns.append(Column(field_name, col_type, nullable=True))

        table = Table(table_name, self.sa_metadata,
                      *columns,
                      Index(f"ix_{table_name}_from", "from_id"),
                      Index(f"ix_{table_name}_to", "to_id"))
        return table

    def _create_data_table(self, config: dict) -> Table:
        """Create a typed data table for melted parquet data with proper FKs."""
        table_name = config["table_name"]
        parent_table = config["parent_table"]
        analyte = config["analyte"]
        sample = config["sample"]

        columns = [
            Column(f"{parent_table}_id", String(255), ForeignKey(f"{parent_table}.id"), nullable=False),
            Column(f"{analyte['table']}_id", String(255), ForeignKey(f"{analyte['table']}.id"), nullable=False),
        ]

        if sample:
            columns.append(
                Column(f"{sample['table']}_id", String(255), ForeignKey(f"{sample['table']}.id"), nullable=False))
        else:
            columns.append(Column("column_name", String(255), nullable=False))

        columns.append(Column("value", Float, nullable=True))

        indexes = [
            Index(f"ix_{table_name}_parent", f"{parent_table}_id"),
            Index(f"ix_{table_name}_analyte", f"{analyte['table']}_id"),
        ]
        if sample:
            indexes.append(Index(f"ix_{table_name}_sample", f"{sample['table']}_id"))

        return Table(table_name, self.sa_metadata, *columns, *indexes)

    # --- Data copying ---

    def _read_collection_paginated(self, collection_name: str, batch_size: int):
        """Read documents from an Arango collection in batches using cursor pagination."""
        db = self.get_db()
        offset = 0
        while True:
            cursor = db.aql.execute(f"""
                FOR doc IN `{collection_name}`
                    SORT doc._key
                    LIMIT {offset}, {batch_size}
                    RETURN doc
            """)
            docs = list(cursor)
            if not docs:
                break
            yield docs
            offset += batch_size
            if len(docs) < batch_size:
                break

    def _copy_document_collection(self, engine, collection_name: str, table: Table,
                                  child_tables: dict, schema: dict, batch_size: int):
        """Copy a document collection from Arango to MySQL."""
        fields = {k: v for k, v in schema["fields"].items() if k not in _SKIP_FIELDS}
        list_fields = {k for k, v in fields.items() if isinstance(v, dict) and v.get("type") == "list"}
        scalar_fields = set(fields.keys()) - list_fields
        total = 0

        for docs in self._read_collection_paginated(collection_name, batch_size):
            rows = []
            child_rows = {field: [] for field in list_fields}

            for doc in docs:
                row = {}
                for field in scalar_fields:
                    val = doc.get(field)
                    if isinstance(val, dict):
                        import json
                        val = json.dumps(val)
                    row[field] = val
                rows.append(row)

                doc_id = doc.get("id")
                for list_field in list_fields:
                    values = doc.get(list_field)
                    if not values or not isinstance(values, list):
                        continue
                    item_schema = fields[list_field].get("item_type")
                    if isinstance(item_schema, dict) and item_schema.get("type") == "object":
                        for item in values:
                            if isinstance(item, dict):
                                child_rows[list_field].append({"parent_id": doc_id, **item})
                    else:
                        for item in values:
                            child_rows[list_field].append({"parent_id": doc_id, "value": item})

            with engine.connect() as conn:
                if rows:
                    conn.execute(table.insert(), rows)
                for field, c_rows in child_rows.items():
                    if c_rows and field in child_tables:
                        conn.execute(child_tables[field].insert(), c_rows)
                conn.commit()

            total += len(docs)

        print(f"  {collection_name}: {total} rows")

    def _copy_edge_collection(self, engine, collection_name: str, table: Table,
                              schema: dict, batch_size: int):
        """Copy an edge collection from Arango to MySQL."""
        edge_fields = {k for k, v in schema.get("fields", {}).items()
                       if isinstance(v, str) and k not in _SKIP_FIELDS}
        total = 0

        for docs in self._read_collection_paginated(collection_name, batch_size):
            rows = []
            for doc in docs:
                row = {
                    "from_id": doc.get("start_id"),
                    "to_id": doc.get("end_id"),
                }
                for field in edge_fields:
                    row[field] = doc.get(field)
                rows.append(row)

            with engine.connect() as conn:
                if rows:
                    conn.execute(table.insert(), rows)
                conn.commit()

            total += len(docs)

        print(f"  {collection_name}: {total} edges")

    # --- Parquet data melting ---

    def _melt_parquet_data(self, engine, data_tables: dict):
        """Melt parquet files into typed data tables with proper FKs."""
        import pyarrow.parquet as pq
        import pandas as pd

        db = self.get_db()

        # Group configs by parent collection
        configs_by_parent = {}
        for table_name, (table, config) in data_tables.items():
            parent = config["parent_collection"]
            configs_by_parent.setdefault(parent, []).append((table, config))

        for parent_coll, configs in configs_by_parent.items():
            # Pre-fetch which documents belong to which analyte type
            # by querying each analyte edge collection for distinct start_ids
            doc_to_config = {}
            for table, config in configs:
                edge_name = config["analyte"]["edge"]
                if not db.has_collection(edge_name):
                    continue
                cursor = db.aql.execute(f"FOR e IN `{edge_name}` RETURN DISTINCT e.start_id")
                for doc_id in cursor:
                    doc_to_config[doc_id] = (table, config)

            # Read all documents with file_reference
            cursor = db.aql.execute(f"""
                FOR doc IN `{parent_coll}`
                    FILTER doc.file_reference != null
                    RETURN {{id: doc.id, file_reference: doc.file_reference}}
            """)

            for doc in cursor:
                doc_id = doc["id"]
                file_ref = doc["file_reference"]

                if doc_id not in doc_to_config:
                    print(f"  Warning: no analyte edges found for {doc_id}, skipping")
                    continue

                if not os.path.exists(file_ref):
                    print(f"  Warning: parquet file not found: {file_ref}")
                    continue

                table, config = doc_to_config[doc_id]
                parent_table = config["parent_table"]
                analyte_table = config["analyte"]["table"]
                sample = config["sample"]

                df = pq.read_table(file_ref).to_pandas()
                index_name = df.index.name or "index"

                melted = df.reset_index().melt(
                    id_vars=[index_name],
                    var_name="col_id",
                    value_name="value"
                )

                rows = []
                for _, r in melted.iterrows():
                    row = {
                        f"{parent_table}_id": doc_id,
                        f"{analyte_table}_id": str(r[index_name]),
                        "value": float(r["value"]) if pd.notna(r["value"]) else None,
                    }
                    if sample:
                        row[f"{sample['table']}_id"] = str(r["col_id"])
                    else:
                        row["column_name"] = str(r["col_id"])
                    rows.append(row)

                if rows:
                    with engine.connect() as conn:
                        for i in range(0, len(rows), 10000):
                            conn.execute(table.insert(), rows[i:i + 10000])
                        conn.commit()
                    print(f"  {config['table_name']}: {len(rows)} values from {doc_id}")