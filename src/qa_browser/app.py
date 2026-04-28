import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List
from urllib.parse import quote as url_quote, urlencode

import urllib3
import yaml
from arango import ArangoClient
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.engine import Engine

import uvicorn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="QA Browser")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["root_path"] = ""

# -- Global state set at startup --
_client: Optional[ArangoClient] = None
_credentials: dict = {}
_mysql_credentials: dict = {}
_mysql_sources: dict = {}
_mysql_db_engines: dict = {}
_mysql_inspector_cache: dict = {}   # db_name -> CachableInspector data
_minio_credentials: dict = {}
_demo_queries_enabled = os.getenv("QA_BROWSER_ENABLE_POUNCE_DEMOS", "").lower() in {
    "1", "true", "yes", "on"
}


def _slugify_mysql_source(value: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "mysql"


def _register_mysql_source(source_id: str, creds: dict, label: Optional[str] = None):
    _mysql_sources[source_id] = {
        "credentials": creds or {},
        "label": label or source_id,
    }


def _get_mysql_source(source_id: str) -> dict:
    source = _mysql_sources.get(source_id)
    if source:
        return source
    if source_id == "default" and _mysql_credentials:
        return {"credentials": _mysql_credentials, "label": "default"}
    raise KeyError(f"Unknown MySQL source: {source_id}")


def _get_parquet_buffer(file_ref: str):
    """Fetch a parquet file from MinIO and return (BytesIO, size_bytes).

    file_ref must be an s3:// URI produced by the ETL pipeline.
    Returns (None, None) if the file cannot be fetched.
    """
    if not file_ref.startswith("s3://"):
        return None, None
    try:
        import io
        import boto3
        from botocore.client import Config

        without_prefix = file_ref[len("s3://"):]
        bucket, key = without_prefix.split("/", 1)
        endpoint = _minio_credentials.get("internal_url") or _minio_credentials.get("url")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=_minio_credentials.get("user"),
            aws_secret_access_key=_minio_credentials.get("password"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            verify=False,
        )
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read()
        return io.BytesIO(data), response["ContentLength"]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {file_ref} from MinIO: {e}") from e


def get_client() -> ArangoClient:
    global _client
    if _client is None:
        url = _credentials.get("internal_url") or _credentials.get("url", "http://localhost:8529")
        _client = ArangoClient(hosts=url, request_timeout=120, verify_override=False)
    return _client


def get_db(name: str):
    client = get_client()
    return client.db(name, username=_credentials.get("user", "root"),
                     password=_credentials.get("password", "password"))


def get_sys_db():
    client = get_client()
    return client.db("_system", username=_credentials.get("user", "root"),
                     password=_credentials.get("password", "password"))


def get_mysql_engine(source_id: str = "default") -> Optional[Engine]:
    """Get a MySQL engine (no specific database) for listing databases."""
    try:
        source = _get_mysql_source(source_id)
    except KeyError:
        return None

    credentials = source["credentials"]
    if not credentials:
        return None

    cache_key = f"{source_id}::_root"
    if cache_key not in _mysql_db_engines:
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        user = credentials.get("user", "root")
        password = url_quote(credentials.get("password", ""), safe="")
        _mysql_db_engines[cache_key] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines[cache_key]


def get_mysql_db_engine(db_name: str, source_id: str = "default") -> Engine:
    """Get a MySQL engine scoped to a specific database."""
    source = _get_mysql_source(source_id)
    credentials = source["credentials"]
    cache_key = f"{source_id}::{db_name}"
    if cache_key not in _mysql_db_engines:
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        user = credentials.get("user", "root")
        password = url_quote(credentials.get("password", ""), safe="")
        _mysql_db_engines[cache_key] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines[cache_key]


def get_mysql_inspector(db_name: str, source_id: str = "default"):
    """Return cached schema metadata for a database.

    Cached as a plain dict so we don't hold live Inspector objects.
    Call invalidate_mysql_inspector(db_name) to force a refresh.
    """
    cache_key = f"{source_id}::{db_name}"
    if cache_key not in _mysql_inspector_cache:
        engine = get_mysql_db_engine(db_name, source_id=source_id)
        insp = sa_inspect(engine)
        table_names = insp.get_table_names()
        meta = {}
        for tbl in table_names:
            meta[tbl] = {
                "columns": insp.get_columns(tbl),
                "pk": insp.get_pk_constraint(tbl).get("constrained_columns", []),
                "fks": insp.get_foreign_keys(tbl),
            }
        _mysql_inspector_cache[cache_key] = meta
    return _mysql_inspector_cache[cache_key]


def invalidate_mysql_inspector(db_name: str, source_id: str = "default"):
    """Drop the cached schema for a database so the next request re-fetches it."""
    _mysql_inspector_cache.pop(f"{source_id}::{db_name}", None)


# ── Jinja2 filters ──────────────────────────────────────────────────────────

def json_pretty(value):
    try:
        return json.dumps(value, indent=2, default=str)
    except Exception:
        return str(value)


templates.env.filters["json_pretty"] = json_pretty


def _get_graph_views(db) -> dict:
    if not db.has_collection("metadata_store"):
        return {}
    try:
        store = db.collection("metadata_store")
        doc = store.get("graph_views")
        if not doc:
            return {}
        return doc.get("value", {}).get("views", {}) or {}
    except Exception:
        return {}


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _format_duration_seconds(total_seconds: float | int | None) -> str:
    if total_seconds is None:
        return ""
    total_seconds = int(total_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _format_elapsed(started_at: Optional[str], finished_at: Optional[str] = None) -> str:
    start_dt = _parse_iso_datetime(started_at)
    if start_dt is None:
        return ""
    end_dt = _parse_iso_datetime(finished_at) or datetime.utcnow()
    return _format_duration_seconds(max((end_dt - start_dt).total_seconds(), 0))


def _get_build_status(db) -> Optional[dict]:
    if not db.has_collection("metadata_store"):
        return None
    try:
        cursor = db.aql.execute("""
            FOR d IN metadata_store
                FILTER d.type == "etl_checkpoint"
                SORT d.last_updated DESC
                RETURN d
        """)
        checkpoints = list(cursor)
    except Exception:
        return None

    if not checkpoints:
        return None

    active = next((doc for doc in checkpoints if any(
        meta.get("status") == "running" for meta in (doc.get("adapters") or {}).values()
    )), None)
    checkpoint = active or checkpoints[0]
    adapters = checkpoint.get("adapters") or {}
    rows = []
    completed = 0
    running = 0
    failed = 0
    total = 0
    total_records = 0

    for adapter_name, metadata in sorted(
        adapters.items(),
        key=lambda item: (item[1].get("adapter_position") or 10**9, item[0])
    ):
        status = metadata.get("status", "unknown")
        if status == "completed":
            completed += 1
        elif status == "running":
            running += 1
        elif status == "failed":
            failed += 1
        total = max(total, metadata.get("adapter_total") or 0)
        total_records += metadata.get("records_written") or 0
        rows.append({
            "name": adapter_name,
            "status": status,
            "adapter_position": metadata.get("adapter_position"),
            "adapter_total": metadata.get("adapter_total"),
            "records_written": metadata.get("records_written"),
            "started_at": metadata.get("started_at"),
            "completed_at": metadata.get("completed_at"),
            "failed_at": metadata.get("failed_at"),
            "error_message": metadata.get("error_message"),
            "elapsed": _format_elapsed(
                metadata.get("started_at"),
                metadata.get("completed_at") or metadata.get("failed_at"),
            ),
        })

    if not total:
        total = len(rows)

    etl_meta = None
    try:
        cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
        results = list(cursor)
        if results and results[0]:
            etl_meta = results[0]
    except Exception:
        etl_meta = None

    checkpoint_updated_dt = _parse_iso_datetime(checkpoint.get("last_updated"))
    etl_run_dt = _parse_iso_datetime((etl_meta or {}).get("run_date"))

    if failed > 0:
        overall_status = "failed"
        overall_message = "One or more adapters failed."
    elif running > 0:
        overall_status = "running"
        overall_message = "Adapters are still running."
    elif completed > 0 and completed >= total:
        if etl_run_dt and (checkpoint_updated_dt is None or etl_run_dt >= checkpoint_updated_dt):
            overall_status = "completed"
            overall_message = "ETL and post-processing completed."
        else:
            overall_status = "post_processing"
            overall_message = "Adapters completed. Post-processing or cleanup is still running."
    elif completed > 0:
        overall_status = "partial"
        overall_message = "Some adapters completed, but the build is not finished."
    else:
        overall_status = "unknown"
        overall_message = "Build state is not yet clear from metadata."

    return {
        "run_id": checkpoint.get("run_id"),
        "last_updated": checkpoint.get("last_updated"),
        "etl_run_date": (etl_meta or {}).get("run_date"),
        "adapters": rows,
        "completed": completed,
        "running": running,
        "failed": failed,
        "known_total": total,
        "seen_total": len(rows),
        "total_records": total_records,
        "is_active": running > 0,
        "overall_status": overall_status,
        "overall_message": overall_message,
    }


def _get_collection_schema_entry(db, coll_name: str) -> dict:
    if not db.has_collection("metadata_store"):
        return {}
    try:
        store = db.collection("metadata_store")
        doc = store.get("collection_schemas")
        if not doc:
            return {}
        return (doc.get("collections") or {}).get(coll_name, {}) or {}
    except Exception:
        return {}


def _get_collection_facet_metadata(db, coll_name: str) -> dict:
    schema_entry = _get_collection_schema_entry(db, coll_name)
    facet_metadata = schema_entry.get("facet_metadata") or {}
    category_fields = list(facet_metadata.get("category_fields") or [])
    if not category_fields:
        category_fields = _infer_collection_category_fields(db, coll_name)
    return {
        "category_fields": category_fields,
        "numeric_fields": list(facet_metadata.get("numeric_fields") or []),
    }


def _get_collection_search_metadata(db, coll_name: str) -> dict:
    schema_entry = _get_collection_schema_entry(db, coll_name)
    search_metadata = schema_entry.get("search_metadata") or {}
    text_fields = list(search_metadata.get("text_fields") or [])
    if not text_fields:
        text_fields = _infer_collection_search_fields(db, coll_name, schema_entry=schema_entry)
    return {
        "text_fields": text_fields,
    }


def _infer_collection_category_fields(db, coll_name: str) -> List[str]:
    try:
        coll = db.collection(coll_name)
        indexes = coll.indexes()
    except Exception:
        return []

    # Older graphs only expose index intent indirectly. Restrict the fallback to
    # simple single-field hash indexes and suppress obviously noisy/internal fields.
    suppressed_fields = {
        "_key",
        "_id",
        "_rev",
        "_from",
        "_to",
        "id",
        "xref",
        "resolved_ids",
        "provenance",
        "creation",
        "updates",
    }
    inferred_fields = []
    for index in indexes:
        if index.get("type") != "hash":
            continue
        fields = index.get("fields") or []
        if len(fields) != 1:
            continue
        field = fields[0]
        if not field or field.startswith("_") or field in suppressed_fields:
            continue
        inferred_fields.append(field)
    return sorted(set(inferred_fields))


def _infer_collection_search_fields(db, coll_name: str, schema_entry: dict = None) -> List[str]:
    schema_entry = schema_entry or _get_collection_schema_entry(db, coll_name)
    schema_fields = set((schema_entry.get("fields") or {}).keys())
    preferred_fields = [
        "id",
        "name",
        "symbol",
        "preferred_symbol",
        "description",
        "label",
        "gene_name",
        "type",
        "xref",
        "uniprot_id",
        "ensembl_id",
        "refseq_id",
        "ncbi_id",
    ]
    return [field for field in preferred_fields if field in schema_fields]


def _parse_collection_facet_filters(request: Request, category_fields: List[str]) -> Dict[str, List[str]]:
    allowed_fields = set(category_fields)
    active_filters = {}
    for field in category_fields:
        key = f"facet_{field}"
        values = [value for value in request.query_params.getlist(key) if value != ""]
        if values:
            active_filters[field] = values
    for key in request.query_params.keys():
        if not key.startswith("facet_"):
            continue
        field = key[len("facet_"):]
        if field not in allowed_fields:
            continue
        values = [value for value in request.query_params.getlist(key) if value != ""]
        if values:
            active_filters[field] = values
    return active_filters


def _parse_collection_search_term(request: Request) -> str:
    return request.query_params.get("q", "").strip()


def _build_collection_query_params(page: int, page_size: int, facet_filters: Dict[str, List[str]],
                                   search_term: str = "", overrides: dict = None) -> List[tuple]:
    params = [("page", page), ("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    merged_filters = {field: list(values) for field, values in facet_filters.items()}
    overrides = overrides or {}
    for field, values in overrides.items():
        merged_filters[field] = list(values)
    for field in sorted(merged_filters):
        for value in merged_filters[field]:
            params.append((f"facet_{field}", value))
    return params


def _build_collection_url(db_name: str, coll_name: str, page: int, page_size: int,
                          facet_filters: Dict[str, List[str]], search_term: str = "",
                          overrides: dict = None) -> str:
    params = _build_collection_query_params(page, page_size, facet_filters, search_term=search_term, overrides=overrides)
    query_string = urlencode(params, doseq=True)
    return f"{templates.env.globals['root_path']}/db/{db_name}/collection/{coll_name}?{query_string}"


def _build_collection_stats_url(db_name: str, coll_name: str, facet_filters: Dict[str, List[str]],
                                search_term: str = "") -> str:
    params = []
    if search_term:
        params.append(("q", search_term))
    for field in sorted(facet_filters):
        for value in facet_filters[field]:
            params.append((f"facet_{field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = f"{templates.env.globals['root_path']}/db/{db_name}/collection/{coll_name}/stats"
    return f"{base_url}?{query_string}" if query_string else base_url


def _build_collection_download_url(db_name: str, coll_name: str, page: int, page_size: int,
                                   facet_filters: Dict[str, List[str]], search_term: str = "") -> str:
    params = _build_collection_query_params(page, page_size, facet_filters, search_term=search_term)
    query_string = urlencode(params, doseq=True)
    base_url = f"{templates.env.globals['root_path']}/db/{db_name}/collection/{coll_name}/download.csv"
    return f"{base_url}?{query_string}" if query_string else base_url


def _get_search_constraint_clause(search_fields: List[str], search_term: str, bind_vars: dict,
                                  variable: str = "doc") -> str:
    if not search_term or not search_fields:
        return ""

    bind_vars["search_term"] = search_term.lower()
    field_bind_names = []
    for idx, field in enumerate(search_fields):
        bind_name = f"search_field_{idx}"
        bind_vars[bind_name] = field
        field_bind_names.append(bind_name)

    field_clauses = []
    for bind_name in field_bind_names:
        field_clauses.append(
            "("
            f"HAS({variable}, @{bind_name}) && {variable}[@{bind_name}] != null && "
            "("
            f"IS_ARRAY({variable}[@{bind_name}]) "
            f"? LENGTH(FOR item IN {variable}[@{bind_name}] FILTER CONTAINS(LOWER(TO_STRING(item)), @search_term) RETURN 1) > 0 "
            f": CONTAINS(LOWER(TO_STRING({variable}[@{bind_name}])), @search_term)"
            ")"
            ")"
        )
    return " OR ".join(field_clauses)


def _get_filter_constraint_clause(filter_settings: Dict[str, List[str]], bind_vars: dict, variable: str = "doc") -> str:
    clauses = []
    for idx, (field, values) in enumerate(filter_settings.items()):
        field_bind = f"facet_field_{idx}"
        values_bind = f"facet_values_{idx}"
        bind_vars[field_bind] = field
        bind_vars[values_bind] = [_coerce_facet_filter_value(value) for value in values]
        clauses.append(
            "("
            f"IS_ARRAY({variable}[@{field_bind}]) "
            f"? LENGTH(INTERSECTION({variable}[@{field_bind}], @{values_bind})) > 0 "
            f": {variable}[@{field_bind}] IN @{values_bind}"
            ")"
        )
    return " AND ".join(clauses)


def _get_facet_clause(field_bind_name: str, top_bind_name: str, variable: str = "doc") -> str:
    return f"""
        LET values = (
            !HAS({variable}, @{field_bind_name}) || {variable}[@{field_bind_name}] == null
                ? [null]
                : (IS_ARRAY({variable}[@{field_bind_name}]) ? UNIQUE({variable}[@{field_bind_name}]) : [{variable}[@{field_bind_name}]])
        )
            FOR item IN values
                COLLECT value = item WITH COUNT INTO count
                SORT count DESC, value
                LIMIT @{top_bind_name}
                RETURN {{ value, count }}"""


def _normalize_facet_value(value):
    return "null" if value is None else str(value)


def _format_facet_value(value):
    return "missing" if value is None else str(value)


def _coerce_facet_filter_value(value: str):
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    return value


def _build_collection_constraints(active_filters: Dict[str, List[str]], search_fields: List[str],
                                  search_term: str, bind_vars: dict, variable: str = "doc") -> str:
    clauses = []
    filter_clause = _get_filter_constraint_clause(active_filters, bind_vars, variable=variable)
    if filter_clause:
        clauses.append(f"({filter_clause})")
    search_clause = _get_search_constraint_clause(search_fields, search_term, bind_vars, variable=variable)
    if search_clause:
        clauses.append(f"({search_clause})")
    return " AND ".join(clauses)


def _build_collection_facet_panels(db, db_name: str, coll_name: str, page_size: int,
                                   category_fields: List[str], active_filters: Dict[str, List[str]],
                                   search_fields: List[str], search_term: str,
                                   top: int = 20) -> List[dict]:
    panels = []
    ordered_fields = sorted(
        category_fields,
        key=lambda field: (0 if active_filters.get(field) else 1, field),
    )
    for field in ordered_fields:
        other_filters = {k: v for k, v in active_filters.items() if k != field}
        bind_vars = {
            "facet_field": field,
            "facet_top": top,
        }
        filter_clause = _build_collection_constraints(
            active_filters=other_filters,
            search_fields=search_fields,
            search_term=search_term,
            bind_vars=bind_vars,
        )
        query = f"""
            FOR doc IN `{coll_name}`
                {f"FILTER {filter_clause}" if filter_clause else ""}
                {_get_facet_clause('facet_field', 'facet_top')}
        """
        rows = list(db.aql.execute(query, bind_vars=bind_vars))
        selected_values = set(active_filters.get(field, []))
        facet_values = []
        for row in rows:
            normalized_value = _normalize_facet_value(row.get("value"))
            updated_values = sorted(selected_values ^ {normalized_value})
            overrides = dict(active_filters)
            if updated_values:
                overrides[field] = updated_values
            else:
                overrides[field] = []
            facet_values.append({
                "value": normalized_value,
                "label": _format_facet_value(row.get("value")),
                "count": row.get("count", 0),
                "selected": normalized_value in selected_values,
                "href": _build_collection_url(
                    db_name=db_name,
                    coll_name=coll_name,
                    page=1,
                    page_size=page_size,
                    facet_filters=active_filters,
                    search_term=search_term,
                    overrides=overrides,
                ),
            })
        panels.append({
            "field": field,
            "title": field.replace("_", " "),
            "values": facet_values,
            "selected_values": sorted(selected_values),
            "clear_href": _build_collection_url(
                db_name=db_name,
                coll_name=coll_name,
                page=1,
                page_size=page_size,
                facet_filters=active_filters,
                search_term=search_term,
                overrides={field: []},
            ),
        })
    return panels


def _build_active_filter_summary(db_name: str, coll_name: str, page_size: int,
                                 active_filters: Dict[str, List[str]], search_term: str = "") -> List[dict]:
    summaries = []
    for field in sorted(active_filters):
        selected_values = list(active_filters[field])
        values = []
        for value in selected_values:
            remaining_values = [item for item in selected_values if item != value]
            overrides = dict(active_filters)
            if remaining_values:
                overrides[field] = remaining_values
            else:
                overrides[field] = []
            values.append({
                "label": _format_facet_value(None if value == "null" else value),
                "remove_href": _build_collection_url(
                    db_name=db_name,
                    coll_name=coll_name,
                    page=1,
                    page_size=page_size,
                    facet_filters=active_filters,
                    search_term=search_term,
                    overrides=overrides,
                ),
            })
        summaries.append({
            "field": field,
            "values": values,
            "clear_href": _build_collection_url(
                db_name=db_name,
                coll_name=coll_name,
                page=1,
                page_size=page_size,
                facet_filters=active_filters,
                search_term=search_term,
                overrides={field: []},
            ),
        })
    return summaries


def _build_browser_home_context(request: Request) -> dict:
    # Arango databases
    arango_databases = []
    arango_url = _credentials.get("url", "")
    if _credentials:
        try:
            sys_db = get_sys_db()
            arango_databases = [db for db in sys_db.databases() if not db.startswith("_")]
        except Exception:
            pass

    # MySQL databases
    mysql_sources = []
    mysql_databases = []
    system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
    for source_id, source in _mysql_sources.items():
        credentials = source["credentials"]
        engine = get_mysql_engine(source_id)
        if not engine:
            continue
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        mysql_url = f"{host}:{port}"
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SHOW DATABASES"))
                dbs = [row[0] for row in result if row[0] not in system_dbs]
                print(f"MySQL databases for {source_id}: {dbs}")
                mysql_databases.extend(dbs)
                mysql_sources.append({
                    "id": source_id,
                    "label": source["label"],
                    "url": mysql_url,
                    "databases": dbs,
                })
        except Exception:
            print(f"Failed to connect to MySQL source {source_id}:", sys.exc_info()[1])
            pass

    return {
        "request": request,
        "databases": arango_databases,
        "arango_url": arango_url,
        "mysql_databases": mysql_databases,
        "mysql_sources": mysql_sources,
        "demo_queries_enabled": _demo_queries_enabled,
    }


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "home.html", {
        "request": request,
    })


@app.get("/qa-browser", response_class=HTMLResponse)
async def qa_browser_home(request: Request):
    return templates.TemplateResponse(
        request,
        "qa_browser_home.html",
        _build_browser_home_context(request),
    )


@app.get("/db/{db_name}", response_class=HTMLResponse)
async def dashboard(request: Request, db_name: str):
    db = get_db(db_name)
    collections_raw = db.collections()
    collections = []
    for coll in collections_raw:
        if coll["name"].startswith("_"):
            continue
        coll_type = coll.get("type")
        is_edge = coll_type in ("edge", 3)
        count_cursor = db.aql.execute(
            f"FOR doc IN `{coll['name']}` COLLECT WITH COUNT INTO c RETURN c"
        )
        count = list(count_cursor)[0]
        collections.append({
            "name": coll["name"],
            "type": "edge" if is_edge else "document",
            "count": count,
        })
    collections.sort(key=lambda c: c["name"])

    # Edge definitions for schema summary
    edge_defs = []
    if db.has_graph("graph"):
        graph = db.graph("graph")
        edge_defs = graph.edge_definitions()

    # ETL metadata
    etl_meta = None
    if db.has_collection("metadata_store"):
        try:
            cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
            results = list(cursor)
            if results and results[0]:
                etl_meta = results[0]
        except Exception:
            pass

    graph_views = _get_graph_views(db)
    return templates.TemplateResponse(request, "dashboard.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
        "edge_defs": edge_defs,
        "etl_meta": etl_meta,
        "graph_views": graph_views,
        "doc_count": sum(c["count"] for c in collections if c["type"] == "document"),
        "edge_count": sum(c["count"] for c in collections if c["type"] == "edge"),
    })


@app.get("/db/{db_name}/build-status", response_class=HTMLResponse)
async def build_status_page(request: Request, db_name: str):
    db = get_db(db_name)
    build_status = _get_build_status(db)
    return templates.TemplateResponse(request, "build_status.html", {
        "request": request,
        "db_name": db_name,
        "build_status": build_status,
    })


@app.get("/db/{db_name}/view/{view_id}")
async def execute_graph_view(db_name: str, view_id: str):
    db = get_db(db_name)
    graph_views = _get_graph_views(db)
    graph_view = graph_views.get(view_id)

    if not graph_view:
        return HTMLResponse(f"Graph view '{view_id}' not found.", status_code=404)

    if graph_view.get("query_language") != "aql":
        return HTMLResponse("Only AQL graph views are supported.", status_code=400)

    if graph_view.get("output_format") != "csv":
        return HTMLResponse("Only CSV graph views are supported.", status_code=400)

    query = graph_view.get("query")
    columns = graph_view.get("columns") or []
    if not query or not columns:
        return HTMLResponse("Graph view is missing query or columns metadata.", status_code=400)

    try:
        rows = list(db.aql.execute(query, max_runtime=60))
    except Exception as exc:
        return HTMLResponse(f"Failed to execute graph view '{view_id}': {exc}", status_code=500)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        normalized_row = {}
        for column in columns:
            value = row.get(column) if isinstance(row, dict) else None
            if isinstance(value, (dict, list)):
                normalized_row[column] = json.dumps(value, default=str)
            elif value is None:
                normalized_row[column] = ""
            else:
                normalized_row[column] = value
        writer.writerow(normalized_row)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={view_id}.csv"},
    )


@app.get("/db/{db_name}/collection/{coll_name}", response_class=HTMLResponse)
async def collection_browser(request: Request, db_name: str, coll_name: str,
                              page: int = 1, page_size: int = 25):
    db = get_db(db_name)
    skip = (page - 1) * page_size
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )

    # Use AQL for both count and list so they always agree
    count_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            COLLECT WITH COUNT INTO c
            RETURN c
    """
    count_cursor = db.aql.execute(count_query, bind_vars=filter_bind_vars)
    total = list(count_cursor)[0]
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    skip = (page - 1) * page_size
    list_bind_vars = {**filter_bind_vars, "skip": skip, "top": page_size}

    coll = db.collection(coll_name)
    is_edge = coll.properties().get("type") in ("edge", 3)

    # Fetch documents
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            LIMIT @skip, @top
            RETURN doc
    """
    cursor = db.aql.execute(query, bind_vars=list_bind_vars)
    docs = list(cursor)
    # Discover columns from this page of results
    columns = _discover_columns(docs, is_edge)
    facet_panels = _build_collection_facet_panels(
        db=db,
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        category_fields=category_fields,
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
    )
    active_filter_summary = _build_active_filter_summary(
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        active_filters=active_filters,
        search_term=search_term,
    )
    stats_url = _build_collection_stats_url(db_name, coll_name, active_filters, search_term=search_term)
    download_url = _build_collection_download_url(db_name, coll_name, page, page_size, active_filters, search_term=search_term)
    clear_search_url = _build_collection_url(
        db_name=db_name,
        coll_name=coll_name,
        page=1,
        page_size=page_size,
        facet_filters=active_filters,
        search_term="",
    )
    prev_url = None
    next_url = None
    if page > 1:
        prev_url = _build_collection_url(db_name, coll_name, page - 1, page_size, active_filters, search_term=search_term)
    if page < total_pages:
        next_url = _build_collection_url(db_name, coll_name, page + 1, page_size, active_filters, search_term=search_term)

    # HTMX partial rendering
    htmx = request.headers.get("HX-Request") == "true"
    template = "collection_rows.html" if htmx else "collection.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "db_name": db_name,
        "coll_name": coll_name,
        "docs": docs,
        "columns": columns,
        "is_edge": is_edge,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "facet_panels": facet_panels,
        "active_filters": active_filters,
        "active_filter_summary": active_filter_summary,
        "search_term": search_term,
        "search_fields": search_fields,
        "stats_url": stats_url,
        "download_url": download_url,
        "clear_search_url": clear_search_url,
        "prev_url": prev_url,
        "next_url": next_url,
    })


@app.get("/db/{db_name}/collection/{coll_name}/download.csv")
async def collection_download(request: Request, db_name: str, coll_name: str,
                              page: int = 1, page_size: int = 25):
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )

    coll = db.collection(coll_name)
    is_edge = coll.properties().get("type") in ("edge", 3)

    # Match the export columns to the current list-page view by rediscovering
    # columns from the currently visible page, then export all filtered rows.
    page = max(page, 1)
    page_size = max(page_size, 1)
    skip = (page - 1) * page_size
    preview_bind_vars = {**filter_bind_vars, "skip": skip, "top": page_size}
    preview_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            LIMIT @skip, @top
            RETURN doc
    """
    preview_docs = list(db.aql.execute(preview_query, bind_vars=preview_bind_vars))
    columns = _discover_columns(preview_docs, is_edge)
    if not columns:
        columns = ["_key"]

    export_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            RETURN doc
    """
    cursor = db.aql.execute(export_query, bind_vars=filter_bind_vars)

    def generate_csv():
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for doc in cursor:
            row = {column: _normalize_csv_value(doc.get(column)) for column in columns}
            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"{db_name}_{coll_name}.csv"
    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/db/{db_name}/collection/{coll_name}/stats", response_class=HTMLResponse)
async def collection_stats(request: Request, db_name: str, coll_name: str):
    """Field coverage stats for a collection (loaded via HTMX)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )

    count_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            COLLECT WITH COUNT INTO c
            RETURN c
    """
    count_results = list(db.aql.execute(count_query, bind_vars=filter_bind_vars))
    total = count_results[0] if count_results else 0
    sample_size = min(total, 500)
    sample_bind_vars = {**filter_bind_vars, "sample_size": sample_size}

    # Sample documents to discover fields
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT RAND()
            LIMIT @sample_size
            RETURN ATTRIBUTES(doc)
    """
    cursor = db.aql.execute(query, bind_vars=sample_bind_vars)
    all_attrs = list(cursor)

    field_counts = {}
    for attrs in all_attrs:
        for attr in attrs:
            if attr.startswith("_"):
                continue
            field_counts[attr] = field_counts.get(attr, 0) + 1

    stats = []
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        pct = round(100 * count / sample_size, 1) if sample_size > 0 else 0
        stats.append({"field": field, "count": count, "pct": pct, "sample_size": sample_size})

    return templates.TemplateResponse(request, "stats_partial.html", {
        "request": request,
        "stats": stats,
        "sample_size": sample_size,
        "total": total,
    })


@app.get("/db/{db_name}/collection/{coll_name}/doc/{doc_key:path}/parquet-stats", response_class=HTMLResponse)
async def parquet_stats(request: Request, db_name: str, coll_name: str, doc_key: str):
    """Load parquet file stats for a Dataset document (called via HTMX)."""
    db = get_db(db_name)
    coll = db.collection(coll_name)
    error = None
    stats = None

    try:
        doc = coll.get(doc_key)
        file_ref = doc.get("file_reference") if doc else None
        if not file_ref:
            error = "No file_reference found on this document."
        else:
            import pyarrow.parquet as pq
            buf, content_length = _get_parquet_buffer(file_ref)
            if buf is None:
                error = f"Could not fetch parquet file: {file_ref}"
            else:
                pf = pq.ParquetFile(buf)
                metadata = pf.metadata

                # Read into pandas for descriptive stats
                table = pf.read()
                df = table.to_pandas()

                col_stats = []
                for col_name in df.columns:
                    col = df[col_name]
                    info = {"name": col_name, "dtype": str(col.dtype), "non_null": int(col.count())}
                    if col.dtype.kind in ("f", "i", "u"):  # numeric
                        info["min"] = f"{col.min():.4g}"
                        info["max"] = f"{col.max():.4g}"
                        info["mean"] = f"{col.mean():.4g}"
                        info["std"] = f"{col.std():.4g}"
                    else:
                        info["unique"] = int(col.nunique())
                    col_stats.append(info)

                stats = {
                    "file_path": file_ref,
                    "file_size_mb": round(content_length / (1024 * 1024), 2),
                    "num_rows": metadata.num_rows,
                    "num_columns": metadata.num_columns,
                    "num_row_groups": metadata.num_row_groups,
                    "index_name": df.index.name,
                    "index_count": len(df.index),
                    "columns": col_stats,
                    "head": df.head(5).to_html(classes="parquet-table", border=0),
                }
    except Exception as e:
        error = str(e)

    return templates.TemplateResponse(request, "parquet_stats.html", {
        "request": request,
        "stats": stats,
        "error": error,
    })


@app.get("/db/{db_name}/collection/{coll_name}/doc/{doc_key:path}", response_class=HTMLResponse)
async def document_detail(request: Request, db_name: str, coll_name: str, doc_key: str):
    db = get_db(db_name)
    coll = db.collection(coll_name)

    try:
        doc = coll.get(doc_key)
    except Exception:
        doc = None

    is_edge = coll.properties().get("type") in ("edge", 3) if doc else False

    # Parse provenance/sources for nicer display
    sources = []
    if doc and "sources" in doc and doc["sources"]:
        for src in doc["sources"]:
            if isinstance(src, str) and "\t" in src:
                parts = src.split("\t")
                sources.append({
                    "name": parts[0] if len(parts) > 0 else "",
                    "version": parts[1] if len(parts) > 1 else "",
                    "version_date": parts[2] if len(parts) > 2 else "",
                    "download_date": parts[3] if len(parts) > 3 else "",
                })
            else:
                sources.append({"name": str(src), "version": "", "version_date": "", "download_date": ""})

    # Categorize fields for display
    skip_keys = {"_key", "_id", "_rev", "_from", "_to", "sources", "provenance", "xref", "labels", "creation"}
    scalar_fields = []
    list_fields = []
    nested_fields = []

    if doc:
        for key, val in doc.items():
            if key in skip_keys or key.startswith("_"):
                continue
            if val is None or isinstance(val, (str, int, float, bool)):
                scalar_fields.append((key, val))
            elif isinstance(val, dict):
                nested_fields.append((key, val))
            elif isinstance(val, list):
                if val and isinstance(val[0], dict):
                    nested_fields.append((key, val))
                elif val:
                    list_fields.append((key, val))
                else:
                    scalar_fields.append((key, val))
            else:
                scalar_fields.append((key, val))

    # Find connected nodes via graph traversal (vertex nodes only)
    outgoing_linked_groups = []
    incoming_linked_groups = []
    linked_aql = ""
    if doc and not is_edge and db.has_graph("graph"):
        try:
            _SAMPLE_LIMIT = 20
            linked_aql = (
                f"LET outgoing_counts = (\n"
                f"    FOR v, e IN 1..1 OUTBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt\n"
                f"    RETURN {{coll, edge_coll, cnt}}\n"
                f")\n"
                f"LET incoming_counts = (\n"
                f"    FOR v, e IN 1..1 INBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt\n"
                f"    RETURN {{coll, edge_coll, cnt}}\n"
                f")\n"
                f"RETURN {{\n"
                f"    outgoing: (\n"
                f"        FOR c IN outgoing_counts\n"
                f"            SORT c.cnt DESC, c.edge_coll\n"
                f"            LET samples = (\n"
                f"                FOR v2, e2 IN 1..1 OUTBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"                FILTER SPLIT(v2._id, '/')[0] == c.coll\n"
                f"                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll\n"
                f"                LIMIT {_SAMPLE_LIMIT}\n"
                f"                RETURN {{key: v2._key, label: v2.name || v2.symbol || v2._key}}\n"
                f"            )\n"
                f"            RETURN {{collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}}\n"
                f"    ),\n"
                f"    incoming: (\n"
                f"        FOR c IN incoming_counts\n"
                f"            SORT c.cnt DESC, c.edge_coll\n"
                f"            LET samples = (\n"
                f"                FOR v2, e2 IN 1..1 INBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"                FILTER SPLIT(v2._id, '/')[0] == c.coll\n"
                f"                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll\n"
                f"                LIMIT {_SAMPLE_LIMIT}\n"
                f"                RETURN {{key: v2._key, label: v2.name || v2.symbol || v2._key}}\n"
                f"            )\n"
                f"            RETURN {{collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}}\n"
                f"    )\n"
                f"}}"
            )
            cursor = db.aql.execute(
                """
                LET outgoing_counts = (
                    FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt
                    RETURN {coll, edge_coll, cnt}
                )
                LET incoming_counts = (
                    FOR v, e IN 1..1 INBOUND @node_id GRAPH 'graph'
                    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt
                    RETURN {coll, edge_coll, cnt}
                )
                RETURN {
                    outgoing: (
                        FOR c IN outgoing_counts
                            SORT c.cnt DESC, c.edge_coll
                            LET samples = (
                                FOR v2, e2 IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                                FILTER SPLIT(v2._id, '/')[0] == c.coll
                                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll
                                LIMIT @sample_limit
                                RETURN {key: v2._key, label: v2.name || v2.symbol || v2._key}
                            )
                            RETURN {collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}
                    ),
                    incoming: (
                        FOR c IN incoming_counts
                            SORT c.cnt DESC, c.edge_coll
                            LET samples = (
                                FOR v2, e2 IN 1..1 INBOUND @node_id GRAPH 'graph'
                                FILTER SPLIT(v2._id, '/')[0] == c.coll
                                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll
                                LIMIT @sample_limit
                                RETURN {key: v2._key, label: v2.name || v2.symbol || v2._key}
                            )
                            RETURN {collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}
                    )
                }
                """,
                bind_vars={"node_id": doc["_id"], "sample_limit": _SAMPLE_LIMIT},
                max_runtime=15,
            )
            linked_result = list(cursor)
            if linked_result:
                outgoing_linked_groups = linked_result[0].get("outgoing", [])
                incoming_linked_groups = linked_result[0].get("incoming", [])
        except Exception:
            pass

    return templates.TemplateResponse(request, "document.html", {
        "request": request,
        "db_name": db_name,
        "coll_name": coll_name,
        "doc": doc,
        "doc_key": doc_key,
        "is_edge": is_edge,
        "sources": sources,
        "scalar_fields": scalar_fields,
        "list_fields": list_fields,
        "nested_fields": nested_fields,
        "outgoing_linked_groups": outgoing_linked_groups,
        "incoming_linked_groups": incoming_linked_groups,
        "linked_aql": linked_aql,
        "this_node_label": f"This {coll_name}",
    })


@app.get("/db/{db_name}/schema", response_class=HTMLResponse)
async def schema_view(request: Request, db_name: str):
    db = get_db(db_name)
    edge_defs = []
    if db.has_graph("graph"):
        graph = db.graph("graph")
        edge_defs = graph.edge_definitions()

    # Build mermaid diagram
    mermaid_lines = ["graph LR"]
    nodes_seen = set()
    for ed in edge_defs:
        edge_name = ed["edge_collection"]
        for frm in ed["from_vertex_collections"]:
            for to in ed["to_vertex_collections"]:
                safe_from = frm.replace(":", "_").replace(" ", "_")
                safe_to = to.replace(":", "_").replace(" ", "_")
                safe_edge = edge_name.replace(":", "_").replace(" ", "_")
                if safe_from not in nodes_seen:
                    mermaid_lines.append(f'    {safe_from}["{frm}"]')
                    nodes_seen.add(safe_from)
                if safe_to not in nodes_seen:
                    mermaid_lines.append(f'    {safe_to}["{to}"]')
                    nodes_seen.add(safe_to)
                mermaid_lines.append(f"    {safe_from} -->|{edge_name}| {safe_to}")

    mermaid_text = "\n".join(mermaid_lines)

    return templates.TemplateResponse(request, "schema.html", {
        "request": request,
        "db_name": db_name,
        "edge_defs": edge_defs,
        "mermaid_text": mermaid_text,
    })


@app.get("/db/{db_name}/aql", response_class=HTMLResponse)
async def aql_page(request: Request, db_name: str):
    return templates.TemplateResponse(request, "aql.html", {
        "request": request,
        "db_name": db_name,
        "results": None,
        "query": "",
        "error": None,
        "columns": [],
    })


@app.post("/db/{db_name}/aql", response_class=HTMLResponse)
async def aql_execute(request: Request, db_name: str, query: str = Form(...)):
    results = None
    error = None
    columns = []
    try:
        db = get_db(db_name)
        cursor = db.aql.execute(query, max_runtime=60)
        results = list(cursor)
        # Auto-detect columns from results
        if results and isinstance(results[0], dict):
            col_set = set()
            for row in results[:50]:
                col_set.update(row.keys())
            columns = sorted(col_set)
    except Exception as e:
        error = str(e)

    htmx = request.headers.get("HX-Request") == "true"
    template = "aql_results.html" if htmx else "aql.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "db_name": db_name,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
    })


# ── MySQL Routes ─────────────────────────────────────────────────────────────

def _mysql_template_context(source_id: str, db_name: str) -> dict:
    source = _get_mysql_source(source_id)
    return {
        "mysql_source_id": source_id,
        "mysql_source_label": source["label"],
        "db_name": db_name,
    }


@app.get("/mysql/{source_id}/{db_name}", response_class=HTMLResponse)
async def mysql_dashboard(request: Request, source_id: str, db_name: str):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    schema_meta = get_mysql_inspector(db_name, source_id=source_id)

    # SHOW TABLE STATUS returns approximate row counts — much faster than COUNT(*) for InnoDB
    with engine.connect() as conn:
        status_rows = conn.execute(text("SHOW TABLE STATUS")).mappings().all()
    row_counts = {r["Name"]: (r["Rows"] or 0) for r in status_rows}

    tables = []
    for table_name, meta in schema_meta.items():
        tables.append({
            "name": table_name,
            "count": row_counts.get(table_name, 0),
            "fk_count": len(meta["fks"]),
        })
    tables.sort(key=lambda t: t["name"])

    fk_defs = []
    for table_name, meta in schema_meta.items():
        for fk in meta["fks"]:
            fk_defs.append({
                "from_table": table_name,
                "from_columns": ", ".join(fk["constrained_columns"]),
                "to_table": fk["referred_table"],
                "to_columns": ", ".join(fk["referred_columns"]),
            })

    return templates.TemplateResponse(request, "mysql_dashboard.html", {
        "request": request,
        "tables": tables,
        "fk_defs": fk_defs,
        "table_count": len(tables),
        "total_rows": sum(t["count"] for t in tables),
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}", response_class=HTMLResponse)
async def mysql_table_browser(request: Request, source_id: str, db_name: str, table_name: str,
                               page: int = 1, page_size: int = 25):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    meta = get_mysql_inspector(db_name, source_id=source_id)[table_name]

    columns_info = meta["columns"]
    pk_cols = meta["pk"]
    column_names = [c["name"] for c in columns_info]

    # Column ordering: PK first, then priority names, then FK columns, then alpha
    fks = meta["fks"]
    fk_col_names = set()
    for fk in fks:
        fk_col_names.update(fk["constrained_columns"])

    priority = list(pk_cols)
    for name in ["name", "symbol", "type", "description"]:
        if name in column_names and name not in priority:
            priority.append(name)
    fk_ordered = [c for c in column_names if c in fk_col_names and c not in priority]
    remaining = sorted(set(column_names) - set(priority) - set(fk_ordered))
    ordered_columns = priority + fk_ordered + remaining

    offset = (page - 1) * page_size
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
        rows = conn.execute(
            text(f"SELECT * FROM `{table_name}` LIMIT :limit OFFSET :offset"),
            {"limit": page_size, "offset": offset}
        ).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    htmx = request.headers.get("HX-Request") == "true"
    template = "mysql_table_rows.html" if htmx else "mysql_table.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "table_name": table_name,
        "rows": [dict(r) for r in rows],
        "columns": ordered_columns,
        "pk_cols": pk_cols,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}/stats", response_class=HTMLResponse)
async def mysql_table_stats(request: Request, source_id: str, db_name: str, table_name: str):
    """Column coverage stats for a MySQL table (loaded via HTMX)."""
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    columns = get_mysql_inspector(db_name, source_id=source_id)[table_name]["columns"]

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()

        stats = []
        for col in columns:
            col_name = col["name"]
            non_null = conn.execute(
                text(f"SELECT COUNT(`{col_name}`) FROM `{table_name}`")
            ).scalar()
            pct = round(100 * non_null / total, 1) if total > 0 else 0
            stats.append({"field": col_name, "count": non_null, "pct": pct})

    return templates.TemplateResponse(request, "stats_partial.html", {
        "request": request,
        "stats": stats,
        "sample_size": total,
        "total": total,
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}/row/{pk_value:path}", response_class=HTMLResponse)
async def mysql_row_detail(request: Request, source_id: str, db_name: str, table_name: str, pk_value: str):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    meta = get_mysql_inspector(db_name, source_id=source_id)[table_name]
    pk_cols = meta["pk"]

    # Parse pk_value: "123" for single PK, "col1=val1/col2=val2" for composite
    where_clauses = []
    bind_params = {}
    if "=" in pk_value:
        parts = pk_value.split("/")
        for part in parts:
            col, val = part.split("=", 1)
            where_clauses.append(f"`{col}` = :pk_{col}")
            bind_params[f"pk_{col}"] = val
    elif pk_cols:
        where_clauses.append(f"`{pk_cols[0]}` = :pk_val")
        bind_params["pk_val"] = pk_value

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=0"

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{table_name}` WHERE {where_sql}"), bind_params)
        row = result.mappings().first()

    # Build FK links for navigation
    fk_links = {}
    if row:
        for fk in meta["fks"]:
            for local_col, ref_col in zip(fk["constrained_columns"], fk["referred_columns"]):
                val = row.get(local_col)
                if val is not None:
                    fk_links[local_col] = {
                        "table": fk["referred_table"],
                        "column": ref_col,
                        "value": val,
                        "url": (
                            f"{templates.env.globals['root_path']}/mysql/{source_id}/{db_name}"
                            f"/table/{fk['referred_table']}/row/{val}"
                        ),
                    }

    # Get column metadata
    columns_info = {c["name"]: str(c["type"]) for c in meta["columns"]}

    return templates.TemplateResponse(request, "mysql_row.html", {
        "request": request,
        "table_name": table_name,
        "row": dict(row) if row else None,
        "pk_value": pk_value,
        "pk_cols": pk_cols,
        "fk_links": fk_links,
        "columns_info": columns_info,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/schema", response_class=HTMLResponse)
async def mysql_schema(request: Request, source_id: str, db_name: str):
    schema_meta = get_mysql_inspector(db_name, source_id=source_id)

    fk_defs = []
    mermaid_lines = ["erDiagram"]

    for table_name, meta in schema_meta.items():
        for fk in meta["fks"]:
            ref_table = fk["referred_table"]
            label = ", ".join(fk["constrained_columns"])
            safe_from = table_name.replace(" ", "_")
            safe_to = ref_table.replace(" ", "_")
            mermaid_lines.append(f'    {safe_from} }}|--|| {safe_to} : "{label}"')
            fk_defs.append({
                "from_table": table_name,
                "from_columns": ", ".join(fk["constrained_columns"]),
                "to_table": ref_table,
                "to_columns": ", ".join(fk["referred_columns"]),
            })

    mermaid_text = "\n".join(mermaid_lines)

    return templates.TemplateResponse(request, "mysql_schema.html", {
        "request": request,
        "fk_defs": fk_defs,
        "mermaid_text": mermaid_text,
        **_mysql_template_context(source_id, db_name),
    })


@app.post("/mysql/{source_id}/{db_name}/refresh-schema", response_class=HTMLResponse)
async def mysql_refresh_schema(request: Request, source_id: str, db_name: str):
    """Bust the schema cache for a database and redirect to dashboard."""
    from fastapi.responses import RedirectResponse
    invalidate_mysql_inspector(db_name, source_id=source_id)
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/{source_id}/{db_name}",
        status_code=303,
    )


@app.get("/mysql/{source_id}/{db_name}/sql", response_class=HTMLResponse)
async def sql_page(request: Request, source_id: str, db_name: str):
    return templates.TemplateResponse(request, "mysql_sql.html", {
        "request": request,
        "results": None,
        "query": "",
        "error": None,
        "columns": [],
        **_mysql_template_context(source_id, db_name),
    })


@app.post("/mysql/{source_id}/{db_name}/sql", response_class=HTMLResponse)
async def sql_execute(request: Request, source_id: str, db_name: str, query: str = Form(...)):
    results = None
    error = None
    columns = []

    # Read-only guard
    query_upper = query.strip().upper()
    allowed = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")
    if not any(query_upper.startswith(kw) for kw in allowed):
        error = "Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed."
    else:
        try:
            engine = get_mysql_db_engine(db_name, source_id=source_id)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                if result.returns_rows:
                    columns = list(result.keys())
                    results = [dict(row) for row in result.mappings().all()]
                else:
                    results = []
        except Exception as e:
            error = str(e)

    htmx = request.headers.get("HX-Request") == "true"
    template = "mysql_sql_results.html" if htmx else "mysql_sql.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{db_name}", response_class=HTMLResponse)
async def mysql_dashboard_default(db_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}",
        status_code=307,
    )


@app.get("/mysql/{db_name}/table/{table_name}", response_class=HTMLResponse)
async def mysql_table_browser_default(db_name: str, table_name: str, page: int = 1, page_size: int = 25):
    return RedirectResponse(
        url=(
            f"{templates.env.globals['root_path']}/mysql/default/{db_name}/table/{table_name}"
            f"?page={page}&page_size={page_size}"
        ),
        status_code=307,
    )


@app.get("/mysql/{db_name}/table/{table_name}/stats", response_class=HTMLResponse)
async def mysql_table_stats_default(db_name: str, table_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/table/{table_name}/stats",
        status_code=307,
    )


@app.get("/mysql/{db_name}/table/{table_name}/row/{pk_value:path}", response_class=HTMLResponse)
async def mysql_row_detail_default(db_name: str, table_name: str, pk_value: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/table/{table_name}/row/{pk_value}",
        status_code=307,
    )


@app.get("/mysql/{db_name}/schema", response_class=HTMLResponse)
async def mysql_schema_default(db_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/schema",
        status_code=307,
    )


@app.post("/mysql/{db_name}/refresh-schema", response_class=HTMLResponse)
async def mysql_refresh_schema_default(db_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/refresh-schema",
        status_code=307,
    )


@app.get("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_page_default(db_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/sql",
        status_code=307,
    )


@app.post("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_execute_default(db_name: str):
    return RedirectResponse(
        url=f"{templates.env.globals['root_path']}/mysql/default/{db_name}/sql",
        status_code=307,
    )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _discover_columns(docs: list, is_edge: bool) -> list:
    """Pick the most useful columns from a set of documents."""
    if not docs:
        return []
    col_set = set()
    for doc in docs:
        col_set.update(doc.keys())

    # Remove internal arango keys except _key
    internal = {"_id", "_rev"}
    if not is_edge:
        internal.update({"_from", "_to"})
    col_set -= internal

    # Order: _key first, then _from/_to for edges, then id, then alpha
    priority = ["_key", "_from", "_to", "id", "name", "symbol", "type", "description"]
    ordered = [c for c in priority if c in col_set]
    remaining = sorted(col_set - set(ordered))
    return ordered + remaining


def _truncate(value, max_len=80):
    """Truncate a value for table display."""
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


def _normalize_csv_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if value is None:
        return ""
    return value


templates.env.filters["truncate_val"] = _truncate


# ── Demo routes ──────────────────────────────────────────────────────────────

if _demo_queries_enabled:
    import src.qa_browser.demo_routes as _demo_module  # noqa: E402
    app.include_router(_demo_module.router)
    _demo_module.set_templates(templates)

# ── POUNCE validation routes ─────────────────────────────────────────────────

import src.qa_browser.pounce_routes as _pounce_module  # noqa: E402
app.include_router(_pounce_module.router)
_pounce_module.set_templates(templates)
_pounce_module.set_mysql_engine_getter(get_mysql_db_engine)
# set_pounce_config is called in main() after args are parsed

# ── Feedback routes ───────────────────────────────────────────────────────────

import src.qa_browser.feedback_routes as _feedback_module  # noqa: E402
app.include_router(_feedback_module.router)
_feedback_module.set_templates(templates)
# set_feedback_file is called in main() after args are parsed


# ── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="QA Browser")
    parser.add_argument("--credentials", "-c",
                        default="./src/use_cases/secrets/local_arangodb.yaml",
                        help="Path to ArangoDB credentials YAML file")
    parser.add_argument("--mysql-credentials", "-m",
                        action="append",
                        default=[],
                        help="Path to a MySQL credentials YAML file; repeat to load multiple MySQL servers")
    parser.add_argument("--minio-credentials", "-s",
                        default=None,
                        help="Path to MinIO credentials YAML file (url, user, password, schema, internal_url)")
    parser.add_argument("--port", "-p", type=int, default=8050)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--root-path", default="", help="ASGI root path for running behind a sub-path proxy (e.g. /odin-qa)")
    parser.add_argument("--pounce-config", "-P", default="./src/use_cases/pounce/pounce.yaml",
                        help="Path to pounce.yaml — used to load resolvers for mapping coverage checks")
    parser.add_argument("--smtp-credentials", "-e",
                        default=None,
                        help="Path to SMTP credentials YAML (host, port, user, password, from_address, to_address, use_tls)")
    parser.add_argument("--feedback-file", "-f",
                        default=None,
                        help="Path to JSON file for storing feedback comments (created if missing)")
    parser.add_argument("--pounce-project-base-url",
                        default="",
                        help="Public base URL for project detail links, e.g. https://pounce-ci.ncats.nih.gov/project")
    args = parser.parse_args()

    global _credentials, _mysql_credentials, _mysql_sources, _minio_credentials
    templates.env.globals["root_path"] = args.root_path.rstrip("/")
    cred_path = Path(args.credentials)
    if cred_path.exists():
        with open(cred_path) as f:
            _credentials = yaml.safe_load(f)
        print(f"Loaded ArangoDB credentials from {cred_path}")
    else:
        print(f"Warning: {cred_path} not found, using defaults")
        _credentials = {"url": "http://localhost:8529", "user": "root", "password": "password"}

    for index, mysql_cred_path in enumerate(args.mysql_credentials, start=1):
        mysql_path = Path(mysql_cred_path)
        if not mysql_path.exists():
            print(f"Warning: MySQL credentials file {mysql_path} not found")
            continue

        with open(mysql_path) as f:
            creds = yaml.safe_load(f) or {}

        source_id = "default" if not _mysql_sources else _slugify_mysql_source(mysql_path.stem)
        if source_id in _mysql_sources:
            suffix = 2
            while f"{source_id}-{suffix}" in _mysql_sources:
                suffix += 1
            source_id = f"{source_id}-{suffix}"

        label = "default" if source_id == "default" else mysql_path.stem
        _register_mysql_source(source_id, creds, label=label)
        if index == 1:
            _mysql_credentials = creds
        print(f"Loaded MySQL credentials from {mysql_path} as source '{source_id}'")

    if _demo_queries_enabled:
        _demo_module.set_mysql_credentials(_mysql_credentials)

    if args.minio_credentials:
        minio_path = Path(args.minio_credentials)
        if minio_path.exists():
            with open(minio_path) as f:
                _minio_credentials = yaml.safe_load(f)
            print(f"Loaded MinIO credentials from {minio_path}")
        else:
            print(f"Warning: MinIO credentials file {minio_path} not found")

    _pounce_module.set_pounce_config(args.pounce_config)
    _pounce_module.set_smtp_config(args.smtp_credentials)
    _pounce_module.set_public_project_base_url(args.pounce_project_base_url)
    _feedback_module.set_feedback_file(args.feedback_file)

    print(f"Starting QA Browser at http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, root_path=args.root_path)


if __name__ == "__main__":
    main()
