import argparse
import csv
import importlib.util
import io
import json
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List
from urllib.parse import quote as url_quote, urlencode

import urllib3
import yaml
from arango import ArangoClient
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.engine import Engine
from starlette.concurrency import run_in_threadpool

import uvicorn

from src.core.data_registry import DataRegistry
from src.models.node import Node
from src.qa_browser.registry_usage import (
    extract_registry_datasets,
    graph_usage_filters,
    graph_usage_styles,
    group_by_source_dataset,
    load_registry_graphs_cached,
    load_graph_registry_usage_cached,
    with_graph_usages,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    _start_resolver_warmup_thread()
    yield


app = FastAPI(title="QA Browser", lifespan=_app_lifespan)
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
_registry_usage_cache: dict = {
    "loaded_at": 0.0,
    "usage_by_registry_id": None,
    "error": None,
}
_registry_graph_cache: dict = {
    "loaded_at": 0.0,
    "graphs": None,
    "error": None,
}
_registry_update_status_cache: dict = {
    "checked_at": None,
    "elapsed_seconds": None,
    "sections": [],
    "error": None,
}
_resolver_instance_cache: dict = {}
_resolver_instance_cache_locks: dict = {}
_resolver_instance_cache_locks_guard = threading.Lock()
_resolver_warmup_thread: Optional[threading.Thread] = None
_resolver_warmup_started = False
_resolver_warmup_status: dict = {
    "started_at": None,
    "completed_at": None,
    "total": 0,
    "warmed": 0,
    "errors": [],
}
_REGISTRY_USAGE_TTL_SECONDS = 60
_RESOLVER_API_MAX_IDS = 1000
_RESOLVER_WARMUP_ENABLED = os.getenv("QA_BROWSER_WARM_RESOLVERS", "1").lower() in {
    "1", "true", "yes", "on"
}
_RESOLVER_WARMUP_ALL_SNAPSHOTS = os.getenv("QA_BROWSER_WARM_ALL_RESOLVER_SNAPSHOTS", "").lower() in {
    "1", "true", "yes", "on"
}
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


def _summarize_checkpoint(checkpoint: dict) -> dict:
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

    run_id = checkpoint.get("run_id")
    run_name = (run_id or checkpoint.get("_key") or "Unknown run").split("/")[-1]
    run_name = run_name.replace(".yaml", "").replace("_", " ")

    return {
        "run_id": run_id,
        "run_label": run_name,
        "last_updated": checkpoint.get("last_updated"),
        "adapters": rows,
        "completed": completed,
        "running": running,
        "failed": failed,
        "known_total": total,
        "seen_total": len(rows),
        "total_records": total_records,
        "is_active": running > 0,
        "status": (
            "failed" if failed > 0 else
            "running" if running > 0 else
            "completed" if completed > 0 and completed >= total else
            "partial" if completed > 0 else
            "unknown"
        ),
    }


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

    run_summaries = [_summarize_checkpoint(checkpoint) for checkpoint in checkpoints]

    etl_meta = None
    try:
        cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
        results = list(cursor)
        if results and results[0]:
            etl_meta = results[0]
    except Exception:
        etl_meta = None

    latest_checkpoint = checkpoints[0]
    checkpoint_updated_dt = _parse_iso_datetime(latest_checkpoint.get("last_updated"))
    etl_run_dt = _parse_iso_datetime((etl_meta or {}).get("run_date"))
    any_failed = any(summary["failed"] > 0 for summary in run_summaries)
    any_running = any(summary["running"] > 0 for summary in run_summaries)
    any_partial = any(
        summary["completed"] > 0 and summary["completed"] < summary["known_total"]
        for summary in run_summaries
    )
    all_complete = bool(run_summaries) and all(
        summary["completed"] >= summary["known_total"] and summary["known_total"] > 0
        for summary in run_summaries
    )

    if any_failed:
        overall_status = "failed"
        overall_message = "One or more ETL runs have failed adapters."
    elif any_running:
        overall_status = "running"
        overall_message = "One or more ETL runs are still running."
    elif all_complete:
        if etl_run_dt and (checkpoint_updated_dt is None or etl_run_dt >= checkpoint_updated_dt):
            overall_status = "completed"
            overall_message = "ETL and post-processing completed."
        else:
            overall_status = "post_processing"
            overall_message = "Adapters completed. Post-processing or cleanup is still running."
    elif any_partial:
        overall_status = "partial"
        overall_message = "Some ETL runs are only partially complete."
    else:
        overall_status = "unknown"
        overall_message = "Build state is not yet clear from metadata."

    return {
        "run_id": latest_checkpoint.get("run_id"),
        "last_updated": latest_checkpoint.get("last_updated"),
        "etl_run_date": (etl_meta or {}).get("run_date"),
        "runs": run_summaries,
        "completed": sum(summary["completed"] for summary in run_summaries),
        "running": sum(summary["running"] for summary in run_summaries),
        "failed": sum(summary["failed"] for summary in run_summaries),
        "known_total": sum(summary["known_total"] for summary in run_summaries),
        "seen_total": sum(summary["seen_total"] for summary in run_summaries),
        "total_records": sum(summary["total_records"] for summary in run_summaries),
        "is_active": any_running,
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


def _build_collection_facets_url(db_name: str, coll_name: str, facet_filters: Dict[str, List[str]],
                                 search_term: str = "", page_size: int = 25) -> str:
    params = [("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    for field in sorted(facet_filters):
        for value in facet_filters[field]:
            params.append((f"facet_{field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = f"{templates.env.globals['root_path']}/db/{db_name}/collection/{coll_name}/facets"
    return f"{base_url}?{query_string}" if query_string else base_url


def _build_collection_facet_url(db_name: str, coll_name: str, field: str,
                                facet_filters: Dict[str, List[str]],
                                search_term: str = "", page_size: int = 25) -> str:
    params = [("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    for filter_field in sorted(facet_filters):
        for value in facet_filters[filter_field]:
            params.append((f"facet_{filter_field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = f"{templates.env.globals['root_path']}/db/{db_name}/collection/{coll_name}/facet/{url_quote(field, safe='')}"
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
    ordered_fields = sorted(
        category_fields,
        key=lambda field: (0 if active_filters.get(field) else 1, field),
    )
    return [
        _build_collection_facet_panel(
            db=db,
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            field=field,
            active_filters=active_filters,
            search_fields=search_fields,
            search_term=search_term,
            top=top,
        )
        for field in ordered_fields
    ]


def _build_collection_facet_panel(db, db_name: str, coll_name: str, page_size: int,
                                  field: str, active_filters: Dict[str, List[str]],
                                  search_fields: List[str], search_term: str,
                                  top: int = 20) -> dict:
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
    return {
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
    }


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


def _get_collection_preview_fields(is_edge: bool, facet_metadata: dict, search_metadata: dict) -> List[str]:
    """Fields to show in collection tables without loading every large document field."""
    fields = ["_key"]
    if is_edge:
        fields.extend(["_from", "_to"])
    priority = [
        "id",
        "name",
        "symbol",
        "preferred_symbol",
        "type",
        "description",
        "label",
        "gene_name",
        "uniprot_id",
        "ensembl_id",
        "refseq_id",
        "ncbi_id",
    ]
    fields.extend(priority)
    fields.extend(sorted(facet_metadata.get("category_fields") or []))
    fields.extend(sorted(facet_metadata.get("numeric_fields") or []))
    fields.extend(search_metadata.get("text_fields") or [])
    return list(dict.fromkeys(fields))


def _build_collection_facet_loaders(db_name: str, coll_name: str, page_size: int,
                                    category_fields: List[str],
                                    active_filters: Dict[str, List[str]],
                                    search_term: str = "") -> List[dict]:
    ordered_fields = sorted(
        category_fields,
        key=lambda field: (0 if active_filters.get(field) else 1, field),
    )
    return [
        {
            "field": field,
            "title": field.replace("_", " "),
            "url": _build_collection_facet_url(
                db_name=db_name,
                coll_name=coll_name,
                field=field,
                facet_filters=active_filters,
                search_term=search_term,
                page_size=page_size,
            ),
        }
        for field in ordered_fields
    ]


def _get_dashboard_collection_summaries(db) -> List[dict]:
    collections = []
    for coll in db.collections():
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
    return collections


def _get_dashboard_collection_shell(db) -> List[dict]:
    collections = []
    for coll in db.collections():
        if coll["name"].startswith("_"):
            continue
        coll_type = coll.get("type")
        is_edge = coll_type in ("edge", 3)
        collections.append({
            "name": coll["name"],
            "type": "edge" if is_edge else "document",
            "count": None,
        })
    collections.sort(key=lambda c: c["name"])
    return collections


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


def _load_registry_catalog() -> tuple[List[dict], List[dict], List[dict], List[dict], Optional[str]]:
    if not _minio_credentials:
        return [], [], [], [], "MinIO credentials are not configured for this QA Browser instance."

    try:
        def load_catalog(registry: DataRegistry):
            snapshots = registry.list_source_snapshots()
            derived_artifacts = registry.list_derived_artifacts()
            external_registrations = registry.list_external_sources()
            resolver_snapshots = registry.list_resolver_snapshots()
            return snapshots, derived_artifacts, external_registrations, resolver_snapshots

        snapshots, derived_artifacts, external_registrations, resolver_snapshots = _with_registry_endpoint_fallback(
            load_catalog,
            error_prefix="Loading registry catalog",
        )
        return snapshots, derived_artifacts, external_registrations, resolver_snapshots, None
    except Exception as exc:
        return [], [], [], [], str(exc)


def _registry_from_minio_credentials(*, use_internal_url: bool):
    if not _minio_credentials:
        raise HTTPException(status_code=503, detail="MinIO credentials are not configured for this QA Browser instance.")
    from src.shared.db_credentials import DBCredentials

    return DataRegistry.from_credentials(
        DBCredentials.from_yaml(_minio_credentials),
        use_internal_url=use_internal_url,
        connect_timeout=2,
        read_timeout=10,
    )


def _registry_endpoint_order() -> List[bool]:
    configured = os.getenv("QA_BROWSER_MINIO_URL_ORDER", "external,internal")
    order = []
    for part in configured.split(","):
        name = part.strip().lower()
        if name in {"internal", "internal_url", "docker"}:
            order.append(True)
        elif name in {"external", "url", "public"}:
            order.append(False)
    return order or [False, True]


def _registry_endpoint_label(use_internal_url: bool) -> str:
    return "internal_url" if use_internal_url else "url"


def _with_registry_endpoint_fallback(operation, *, error_prefix: str):
    last_error = None
    tried = []
    for use_internal_url in _registry_endpoint_order():
        endpoint_label = _registry_endpoint_label(use_internal_url)
        tried.append(endpoint_label)
        try:
            registry = _registry_from_minio_credentials(use_internal_url=use_internal_url)
            return operation(registry)
        except HTTPException:
            raise
        except Exception as exc:
            last_error = exc
            print(f"{error_prefix} failed using MinIO {endpoint_label}: {exc}", flush=True)
    raise RuntimeError(f"{error_prefix} failed using MinIO endpoints {', '.join(tried)}: {last_error}")


def _load_registry_update_inputs(registry: DataRegistry, timeout: int):
    return {
        "source_statuses": registry.check_all_latest_registered(timeout=timeout),
        "external_statuses": registry.check_external_registrations(),
        "derived_statuses": registry.check_derived_artifacts(),
        "resolver_statuses": registry.check_resolvers(),
    }



def _registry_status_category(status: dict) -> str:
    if status.get("error"):
        return "error"
    if not status.get("registered_versions"):
        return "missing"
    if status.get("is_latest_registered") is True:
        return "current"
    if status.get("is_latest_registered") is False:
        return "update_available"
    return "unknown"


def _registry_status_item_label(status: dict) -> str:
    name = status.get("resolver") or status.get("dataset") or ""
    source = status.get("source") or ""
    return f"{source}:{name}" if source and name else source or name


def _summarize_registry_status_section(label: str, statuses: List[dict]) -> dict:
    counts = {
        "current": 0,
        "update_available": 0,
        "missing": 0,
        "unknown": 0,
        "error": 0,
    }
    items = []
    for status in statuses:
        category = _registry_status_category(status)
        counts[category] += 1
        if category != "current":
            detail = status.get("sync_reason") or status.get("error")
            if not detail and category == "unknown":
                detail = "latest version not available from checker"
            items.append({
                "label": _registry_status_item_label(status),
                "category": category,
                "latest_version": status.get("latest_version"),
                "latest_version_date": status.get("latest_version_date"),
                "latest_registered_version": status.get("latest_registered_version"),
                "days_since_last_update": status.get("days_since_last_update"),
                "latest_build_key": status.get("latest_build_key"),
                "sync_reason": detail,
                "error": status.get("error"),
            })
    items.sort(key=lambda item: (
        {"update_available": 0, "missing": 1, "error": 2, "unknown": 3}.get(item["category"], 4),
        item["label"],
    ))
    return {
        "label": label,
        "total": len(statuses),
        "counts": counts,
        "items": items,
    }


def _registry_status_key(status: dict) -> Optional[tuple]:
    source = status.get("source")
    resolver = status.get("resolver")
    dataset = status.get("dataset")
    if source and resolver:
        return "resolver", source, resolver
    if source and dataset:
        return "dataset", source, dataset
    return None


def _graph_dependency_keys(graph: dict) -> List[tuple]:
    keys = []

    def visit_dependency(dependency: dict):
        if not isinstance(dependency, dict):
            return
        source = dependency.get("source")
        dataset = dependency.get("dataset")
        if source and dataset:
            keys.append(("dataset", source, dataset))
        for upstream in dependency.get("derived_from") or []:
            visit_dependency(upstream)

    for adapter in graph.get("adapters") or []:
        for dependency in adapter.get("datasets") or []:
            visit_dependency(dependency)
    for resolver in graph.get("resolvers") or []:
        snapshot = resolver.get("snapshot") or {}
        source = snapshot.get("source")
        resolver_name = resolver.get("name")
        if source and resolver_name:
            keys.append(("resolver", source, resolver_name))
        for dependency in resolver.get("inputs") or []:
            visit_dependency(dependency)
    return keys


def _graph_update_statuses(graphs: List[dict], stale_keys: set, unknown_keys: set) -> List[dict]:
    statuses = []
    for graph in graphs:
        dependency_keys = set(_graph_dependency_keys(graph))
        stale_dependency_count = len(dependency_keys & stale_keys)
        unknown_dependency_count = len(dependency_keys & unknown_keys)
        if stale_dependency_count:
            is_latest_registered = False
            sync_reason = f"{stale_dependency_count} dependency updates available"
        elif unknown_dependency_count:
            is_latest_registered = None
            sync_reason = f"{unknown_dependency_count} dependencies unknown"
        else:
            is_latest_registered = True
            sync_reason = None
        statuses.append({
            "source": "graph",
            "dataset": graph.get("name"),
            "registered_versions": [graph.get("run_date") or "built"],
            "latest_registered_version": graph.get("run_date"),
            "latest_version": None,
            "is_latest_registered": is_latest_registered,
            "sync_reason": sync_reason,
            "error": None,
        })
    return statuses


def _run_registry_update_checks() -> dict:
    timeout = int(os.getenv("QA_BROWSER_REGISTRY_UPDATE_CHECK_TIMEOUT", "20"))
    started = time.time()
    status_inputs = _with_registry_endpoint_fallback(
        lambda registry: _load_registry_update_inputs(registry, timeout),
        error_prefix="Checking registry update status",
    )
    source_statuses = status_inputs["source_statuses"]
    external_statuses = status_inputs["external_statuses"]
    derived_statuses = status_inputs["derived_statuses"]
    resolver_statuses = status_inputs["resolver_statuses"]
    stale_keys = {
        key
        for status in [*source_statuses, *external_statuses, *derived_statuses, *resolver_statuses]
        for key in [_registry_status_key(status)]
        if key and _registry_status_category(status) in {"update_available", "missing"}
    }
    unknown_keys = {
        key
        for status in [*source_statuses, *external_statuses, *derived_statuses, *resolver_statuses]
        for key in [_registry_status_key(status)]
        if key and _registry_status_category(status) in {"unknown", "error"}
    }
    graphs, graph_error = load_registry_graphs_cached(
        credentials=_credentials,
        cache=_registry_graph_cache,
        ttl_seconds=0,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    sections = [
        _summarize_registry_status_section(
            "Source Snapshots",
            source_statuses,
        ),
        _summarize_registry_status_section(
            "External Sources",
            external_statuses,
        ),
        _summarize_registry_status_section(
            "Derived Artifacts",
            derived_statuses,
        ),
        _summarize_registry_status_section(
            "Resolvers",
            resolver_statuses,
        ),
    ]
    if graph_error:
        sections.append(_summarize_registry_status_section("Graphs", [{
            "source": "graph",
            "dataset": "registry graphs",
            "registered_versions": ["unknown"],
            "is_latest_registered": None,
            "error": graph_error,
        }]))
    else:
        sections.append(_summarize_registry_status_section(
            "Graphs",
            _graph_update_statuses(graphs, stale_keys, unknown_keys),
        ))
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "elapsed_seconds": round(time.time() - started, 1),
        "sections": sections,
        "error": None,
    }


def _registry_update_status_context() -> dict:
    return _registry_update_status_cache


def _list_resolver_snapshots_for_warmup() -> List[dict]:
    return _with_registry_endpoint_fallback(
        lambda registry: registry.list_resolver_snapshots(),
        error_prefix="Listing resolver snapshots",
    )


def _latest_resolver_snapshots(resolver_snapshots: List[dict]) -> List[dict]:
    latest_by_resolver = {}
    for snapshot in resolver_snapshots:
        source = snapshot.get("source")
        resolver = snapshot.get("resolver")
        version = snapshot.get("version")
        if not source or not resolver or not version:
            continue
        key = (source, resolver)
        current = latest_by_resolver.get(key)
        if current is None or (
            snapshot.get("created_at") or "",
            snapshot.get("version") or "",
        ) > (
            current.get("created_at") or "",
            current.get("version") or "",
        ):
            latest_by_resolver[key] = snapshot
    return sorted(
        latest_by_resolver.values(),
        key=lambda snapshot: (
            snapshot.get("source") or "",
            snapshot.get("resolver") or "",
            snapshot.get("created_at") or "",
            snapshot.get("version") or "",
        ),
    )


def _warm_resolver_instances_in_background():
    _resolver_warmup_status.update({
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "total": 0,
        "warmed": 0,
        "errors": [],
    })

    try:
        resolver_snapshots = _list_resolver_snapshots_for_warmup()
        if not _RESOLVER_WARMUP_ALL_SNAPSHOTS:
            resolver_snapshots = _latest_resolver_snapshots(resolver_snapshots)
        else:
            resolver_snapshots = sorted(
                resolver_snapshots,
                key=lambda snapshot: (
                    snapshot.get("source") or "",
                    snapshot.get("resolver") or "",
                    snapshot.get("created_at") or "",
                    snapshot.get("version") or "",
                ),
            )
        _resolver_warmup_status["total"] = len(resolver_snapshots)
        scope = "all resolver snapshots" if _RESOLVER_WARMUP_ALL_SNAPSHOTS else "latest resolver snapshots"
        print(f"Starting resolver warmup for {len(resolver_snapshots)} {scope}.")

        for snapshot in resolver_snapshots:
            source = snapshot.get("source")
            resolver = snapshot.get("resolver")
            version = snapshot.get("version")
            if not source or not resolver or not version:
                continue
            try:
                _get_resolver_instance_for_api(source, resolver, version)
                _resolver_warmup_status["warmed"] += 1
                print(f"Warmed resolver {source}:{resolver}:{version}")
            except Exception as exc:
                message = f"{source}:{resolver}:{version}: {exc}"
                _resolver_warmup_status["errors"].append(message)
                print(f"Failed to warm resolver {message}")
    except Exception as exc:
        message = str(exc)
        _resolver_warmup_status["errors"].append(message)
        print(f"Resolver warmup failed: {message}")
    finally:
        _resolver_warmup_status["completed_at"] = datetime.now(timezone.utc).isoformat()
        print(
            "Resolver warmup complete: "
            f"{_resolver_warmup_status['warmed']}/{_resolver_warmup_status['total']} warmed, "
            f"{len(_resolver_warmup_status['errors'])} errors."
        )


def _start_resolver_warmup_thread():
    global _resolver_warmup_thread, _resolver_warmup_started
    if not _RESOLVER_WARMUP_ENABLED:
        print("Resolver warmup disabled by QA_BROWSER_WARM_RESOLVERS.")
        return
    if _resolver_warmup_started:
        return
    if not _minio_credentials:
        print("Resolver warmup skipped because MinIO credentials are not configured.")
        return
    _resolver_warmup_started = True
    _resolver_warmup_thread = threading.Thread(
        target=_warm_resolver_instances_in_background,
        name="qa-browser-resolver-warmup",
        daemon=True,
    )
    _resolver_warmup_thread.start()


def _materialize_resolver_snapshot_for_api(source: str, resolver: str, version: str):
    cache_dir = Path(os.getenv("QA_BROWSER_REGISTRY_CACHE_DIR", "/tmp/ifx-registry-cache"))
    try:
        return _with_registry_endpoint_fallback(
            lambda registry: registry.materialize_resolver_snapshot(source, resolver, version, dest=cache_dir),
            error_prefix=f"Materializing resolver snapshot {source}/{resolver}/{version}",
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Could not materialize resolver snapshot: {exc}") from exc


def _load_class_from_definition(definition: dict):
    module_path = definition.get("import")
    class_name = definition.get("class")
    if not module_path or not class_name:
        raise HTTPException(status_code=500, detail="Resolver snapshot definition is missing import/class metadata.")
    abs_module_path = os.path.abspath(module_path)
    normalized_module_path = os.path.normpath(abs_module_path)
    module_name = (
        "qa_resolver_import__"
        + normalized_module_path.replace(":", "").replace(os.sep, "_").replace(".", "_")
    )
    module = sys.modules.get(module_name)
    if module is None:
        spec = importlib.util.spec_from_file_location(module_name, abs_module_path)
        if spec is None or spec.loader is None:
            raise HTTPException(status_code=500, detail=f"Could not load resolver module {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
    return getattr(module, class_name)


def _get_resolver_instance_for_api(source: str, resolver: str, version: str):
    cache_key = (source, resolver, version)
    if cache_key in _resolver_instance_cache:
        return _resolver_instance_cache[cache_key]

    with _resolver_instance_cache_locks_guard:
        lock = _resolver_instance_cache_locks.setdefault(cache_key, threading.Lock())

    with lock:
        if cache_key in _resolver_instance_cache:
            return _resolver_instance_cache[cache_key]

        resolver_snapshot = _materialize_resolver_snapshot_for_api(source, resolver, version)
        definition = resolver_snapshot.manifest.get("definition") or {}
        accepted_types = list(definition.get("accepted_types") or [])
        if not accepted_types:
            raise HTTPException(status_code=400, detail=f"Resolver snapshot {resolver_snapshot.snapshot_id} has no accepted_types.")
        type_sensitive = bool(definition.get("type_sensitive"))
        resolver_class = _load_class_from_definition(definition)
        resolver_instance = resolver_class(
            resolver_snapshot=resolver_snapshot,
            types=accepted_types,
        )
        _resolver_instance_cache[cache_key] = resolver_snapshot, resolver_instance, accepted_types, type_sensitive
        return _resolver_instance_cache[cache_key]


def _node_class_for_api_type(input_type: str):
    return type(input_type, (Node,), {})


def _nodes_for_resolver_api(input_type: str, ids: List[str]) -> List[Node]:
    node_class = _node_class_for_api_type(input_type)
    nodes = []
    for value in ids:
        node = node_class(id=value)
        setattr(node, "name", value)
        setattr(node, "text", value)
        nodes.append(node)
    return nodes


def _serialize_id_match(match) -> dict:
    equivalent_ids = []
    for equivalent_id in match.equivalent_ids or []:
        if hasattr(equivalent_id, "id_str"):
            equivalent_ids.append(equivalent_id.id_str())
        else:
            equivalent_ids.append(str(equivalent_id))
    return {
        "input": match.input,
        "match": match.match,
        "equivalent_ids": equivalent_ids,
        "context": list(match.context or []),
    }


def _resolve_ids_for_type(resolver_instance, input_type: str, ids: List[str]) -> List[dict]:
    nodes = _nodes_for_resolver_api(input_type, ids)
    raw_matches = resolver_instance.resolve_internal(nodes)
    return [
        {
            "input": input_id,
            "matches": [
                _serialize_id_match(match)
                for match in raw_matches.get(input_id, []) or []
            ],
        }
        for input_id in ids
    ]


def _resolver_prefix_counts_for_api(resolver_instance) -> List[dict]:
    counts = []
    for row in resolver_instance.get_prefix_counts() or []:
        if not isinstance(row, dict):
            continue
        prefix = str(row.get("prefix") or "").strip()
        if not prefix:
            continue
        raw_count = row.get("count")
        count = raw_count if isinstance(raw_count, int) else None
        counts.append({"prefix": prefix, "count": count})
    return sorted(
        counts,
        key=lambda row: (
            -(row["count"] if isinstance(row.get("count"), int) else -1),
            row["prefix"].lower(),
        ),
    )


def _resolver_example_ids_for_api(resolver_instance) -> List[str]:
    return [
        str(example_id)
        for example_id in resolver_instance.get_example_ids(limit=5) or []
        if str(example_id).strip()
    ]


def _normalize_resolver_api_ids(payload: dict) -> List[str]:
    raw_ids = payload.get("ids")
    if raw_ids is None and "id" in payload:
        raw_ids = [payload.get("id")]
    if isinstance(raw_ids, str):
        raw_ids = [raw_ids]
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="Request body must include ids as a string or list of strings.")
    ids = [str(value).strip() for value in raw_ids if str(value).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="At least one non-empty id is required.")
    if len(ids) > _RESOLVER_API_MAX_IDS:
        raise HTTPException(status_code=400, detail=f"At most {_RESOLVER_API_MAX_IDS} ids can be resolved in one request.")
    return ids


def _resolver_snapshots_for_page(
    resolver_snapshots: List[dict],
    source: str,
    resolver: str,
) -> List[dict]:
    snapshots = [
        snapshot
        for snapshot in resolver_snapshots
        if snapshot.get("source") == source and snapshot.get("resolver") == resolver
    ]
    return sorted(
        snapshots,
        key=lambda snapshot: (snapshot.get("created_at") or "", snapshot.get("version") or ""),
        reverse=True,
    )


def _resolver_resolve_payload_for_api(source: str, resolver: str, version: str, payload: dict) -> dict:
    ids = _normalize_resolver_api_ids(payload)
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    requested_type = payload.get("input_type") or payload.get("type")
    if requested_type is not None:
        requested_type = str(requested_type).strip()
        if requested_type not in accepted_types:
            raise HTTPException(
                status_code=400,
                detail=f"input_type must be one of {accepted_types}; got {requested_type!r}",
            )
        input_types = [requested_type]
        result_specs = [(requested_type, requested_type)]
    elif type_sensitive:
        input_types = accepted_types
        result_specs = [(input_type, input_type) for input_type in accepted_types]
    else:
        input_types = ["Any accepted type"]
        result_specs = [(input_types[0], accepted_types[0])]

    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "input_types": input_types,
        "results_by_type": {
            result_label: _resolve_ids_for_type(resolver_instance, node_type, ids)
            for result_label, node_type in result_specs
        },
    }


def _resolver_prefix_counts_payload_for_api(source: str, resolver: str, version: str) -> dict:
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    try:
        prefix_counts = _resolver_prefix_counts_for_api(resolver_instance)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not load resolver prefix counts: {exc}") from exc
    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "prefix_counts": prefix_counts,
    }


def _resolver_examples_payload_for_api(source: str, resolver: str, version: str) -> dict:
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    try:
        example_ids = _resolver_example_ids_for_api(resolver_instance)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not load resolver examples: {exc}") from exc
    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "example_ids": example_ids,
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


@app.get("/registry", response_class=HTMLResponse)
def registry_home(request: Request):
    snapshots, derived_artifacts, external_registrations, _resolver_snapshots, registry_error = _load_registry_catalog()
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_filters = graph_usage_filters(graph_usage_by_registry_id)
    graph_styles = graph_usage_styles(graph_filters)

    snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in snapshots
    ]
    derived_artifact_list = [
        with_graph_usages(artifact, graph_usage_by_registry_id)
        for artifact in derived_artifacts
    ]
    external_registration_list = [
        with_graph_usages(registration, graph_usage_by_registry_id)
        for registration in external_registrations
    ]

    grouped_source_list = []
    grouped_derived_source_list = []
    registry_stats = {
        "source_count": 0,
        "dataset_count": 0,
        "derived_count": 0,
        "external_count": 0,
        "total_size": "",
    }
    if not registry_error:
        grouped_source_list = group_by_source_dataset(snapshot_list, "snapshots")
        grouped_derived_source_list = group_by_source_dataset(derived_artifact_list, "artifacts")
        total_size_bytes = (
            sum(snapshot.get("total_size_bytes", 0) or 0 for snapshot in snapshots)
            + sum(artifact.get("total_size_bytes", 0) or 0 for artifact in derived_artifacts)
        )
        registry_stats = {
            "source_count": len(grouped_source_list),
            "dataset_count": sum(len(group["datasets"]) for group in grouped_source_list),
            "derived_count": len(derived_artifacts),
            "external_count": len(external_registrations),
            "total_size": DataRegistry.format_size(total_size_bytes),
        }

    return templates.TemplateResponse(request, "registry_home.html", {
        "request": request,
        "snapshots": snapshot_list,
        "derived_artifacts": derived_artifact_list,
        "external_registrations": external_registration_list,
        "grouped_sources": grouped_source_list,
        "grouped_derived_sources": grouped_derived_source_list,
        "registry_stats": registry_stats,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_filters": graph_filters,
        "graph_usage_styles": graph_styles,
        "registry_update_status": _registry_update_status_context(),
    })


@app.post("/registry/update-status", response_class=HTMLResponse)
def registry_update_status(return_to: str = Form("/registry")):
    try:
        status = _run_registry_update_checks()
    except Exception as exc:
        status = {
            "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "elapsed_seconds": None,
            "sections": [],
            "error": str(exc),
        }
    _registry_update_status_cache.clear()
    _registry_update_status_cache.update(status)
    if not return_to.startswith("/") or return_to.startswith("//"):
        return_to = "/registry"
    return RedirectResponse(return_to, status_code=303)


@app.get("/registry/resolvers", response_class=HTMLResponse)
def registry_resolvers(request: Request):
    _snapshots, _derived_artifacts, _external_registrations, resolver_snapshots, registry_error = _load_registry_catalog()
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_filters = graph_usage_filters(graph_usage_by_registry_id)
    graph_styles = graph_usage_styles(graph_filters)

    resolver_snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in resolver_snapshots
    ]
    grouped_resolver_list = []
    registry_stats = {
        "source_count": 0,
        "resolver_count": 0,
        "snapshot_count": 0,
        "total_size": "",
    }
    if not registry_error:
        grouped_resolver_list = group_by_source_dataset(resolver_snapshot_list, "snapshots")
        total_size_bytes = sum(snapshot.get("total_size_bytes", 0) or 0 for snapshot in resolver_snapshots)
        registry_stats = {
            "source_count": len(grouped_resolver_list),
            "resolver_count": sum(len(group["datasets"]) for group in grouped_resolver_list),
            "snapshot_count": len(resolver_snapshots),
            "total_size": DataRegistry.format_size(total_size_bytes),
        }

    return templates.TemplateResponse(request, "registry_resolvers.html", {
        "request": request,
        "resolver_snapshots": resolver_snapshot_list,
        "grouped_resolvers": grouped_resolver_list,
        "registry_stats": registry_stats,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_filters": graph_filters,
        "graph_usage_styles": graph_styles,
        "registry_update_status": _registry_update_status_context(),
    })


@app.get("/registry/graphs", response_class=HTMLResponse)
def registry_graphs(request: Request):
    graphs, graph_error = load_registry_graphs_cached(
        credentials=_credentials,
        cache=_registry_graph_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    registry_stats = {
        "graph_count": len(graphs),
        "adapter_count": sum(len(graph.get("adapters") or []) for graph in graphs),
        "resolver_count": sum(len(graph.get("resolvers") or []) for graph in graphs),
        "dependency_count": sum(
            sum(len(adapter.get("datasets") or []) for adapter in graph.get("adapters") or [])
            + sum(
                (1 if resolver.get("snapshot") else 0) + len(resolver.get("inputs") or [])
                for resolver in graph.get("resolvers") or []
            )
            for graph in graphs
        ),
    }

    return templates.TemplateResponse(request, "registry_graphs.html", {
        "request": request,
        "graphs": graphs,
        "registry_stats": registry_stats,
        "graph_error": graph_error,
        "registry_update_status": _registry_update_status_context(),
    })


@app.get("/registry/resolvers/{source}/{resolver}", response_class=HTMLResponse)
def registry_resolver_detail(request: Request, source: str, resolver: str, version: Optional[str] = None):
    _snapshots, _derived_artifacts, _external_registrations, resolver_snapshots, registry_error = _load_registry_catalog()
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_styles = graph_usage_styles(graph_usage_filters(graph_usage_by_registry_id))
    resolver_snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in _resolver_snapshots_for_page(resolver_snapshots, source, resolver)
    ]
    for snapshot in resolver_snapshot_list:
        definition = snapshot.get("definition") or {}
        definition["type_sensitive"] = bool(definition.get("type_sensitive"))
    selected_snapshot = None
    if resolver_snapshot_list:
        selected_snapshot = next(
            (
                snapshot
                for snapshot in resolver_snapshot_list
                if snapshot.get("version") == version
            ),
            resolver_snapshot_list[0],
        )

    return templates.TemplateResponse(request, "registry_resolver_detail.html", {
        "request": request,
        "source": source,
        "resolver": resolver,
        "resolver_snapshots": resolver_snapshot_list,
        "selected_snapshot": selected_snapshot,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_styles": graph_styles,
    })


@app.post("/registry/resolvers/{source}/{resolver}/{version}/resolve")
async def registry_resolver_resolve(source: str, resolver: str, version: str, request: Request):
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object.")

    return await run_in_threadpool(_resolver_resolve_payload_for_api, source, resolver, version, payload)


@app.get("/registry/resolvers/{source}/{resolver}/{version}/prefix-counts")
async def registry_resolver_prefix_counts(source: str, resolver: str, version: str):
    return await run_in_threadpool(_resolver_prefix_counts_payload_for_api, source, resolver, version)


@app.get("/registry/resolvers/{source}/{resolver}/{version}/examples")
async def registry_resolver_examples(source: str, resolver: str, version: str):
    return await run_in_threadpool(_resolver_examples_payload_for_api, source, resolver, version)


@app.get("/db/{db_name}", response_class=HTMLResponse)
def dashboard(request: Request, db_name: str):
    db = get_db(db_name)
    collections = _get_dashboard_collection_shell(db)

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
    registry_datasets = extract_registry_datasets(etl_meta)
    return templates.TemplateResponse(request, "dashboard.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
        "edge_defs": edge_defs,
        "etl_meta": etl_meta,
        "registry_datasets": registry_datasets,
        "graph_views": graph_views,
        "doc_count": None,
        "edge_count": None,
        "counts_url": f"{templates.env.globals['root_path']}/db/{db_name}/collections",
    })


@app.get("/db/{db_name}/collections", response_class=HTMLResponse)
def dashboard_collections(request: Request, db_name: str):
    db = get_db(db_name)
    collections = _get_dashboard_collection_summaries(db)
    return templates.TemplateResponse(request, "dashboard_collections.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
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


@app.get("/db/{db_name}/view/{view_id}/preview", response_class=HTMLResponse)
async def preview_graph_view(request: Request, db_name: str, view_id: str, limit: int = 50):
    db = get_db(db_name)
    graph_views = _get_graph_views(db)
    graph_view = graph_views.get(view_id)

    if not graph_view:
        return HTMLResponse(f"Graph view '{view_id}' not found.", status_code=404)

    if graph_view.get("query_language") != "aql":
        return HTMLResponse("Only AQL graph views are supported.", status_code=400)

    if graph_view.get("output_format") != "jsonl":
        return HTMLResponse("Only JSONL graph views can be previewed.", status_code=400)

    query = graph_view.get("query")
    if not query:
        return HTMLResponse("Graph view is missing query metadata.", status_code=400)

    preview_limit = max(1, min(limit, 200))
    preview_query = f"""
    LET graph_view_rows = (
      {query}
    )
    RETURN {{
      total_count: LENGTH(graph_view_rows),
      rows: (
        FOR row IN graph_view_rows
          LIMIT @limit
          RETURN row
      )
    }}
    """

    try:
        result = next(iter(db.aql.execute(preview_query, bind_vars={"limit": preview_limit}, max_runtime=60)), None)
    except Exception as exc:
        return templates.TemplateResponse(request, "graph_view_preview.html", {
            "request": request,
            "db_name": db_name,
            "view_id": view_id,
            "graph_view": graph_view,
            "rows": [],
            "total_count": None,
            "preview_limit": preview_limit,
            "error": str(exc),
        })

    return templates.TemplateResponse(request, "graph_view_preview.html", {
        "request": request,
        "db_name": db_name,
        "view_id": view_id,
        "graph_view": graph_view,
        "rows": (result or {}).get("rows") or [],
        "total_count": (result or {}).get("total_count"),
        "preview_limit": preview_limit,
        "error": None,
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

    output_format = graph_view.get("output_format")
    if output_format not in {"csv", "jsonl"}:
        return HTMLResponse("Only CSV and JSONL graph views are supported.", status_code=400)

    query = graph_view.get("query")
    columns = graph_view.get("columns") or []
    if not query:
        return HTMLResponse("Graph view is missing query metadata.", status_code=400)
    if output_format == "csv" and not columns:
        return HTMLResponse("CSV graph view is missing columns metadata.", status_code=400)

    try:
        rows = list(db.aql.execute(query, max_runtime=60))
    except Exception as exc:
        return HTMLResponse(f"Failed to execute graph view '{view_id}': {exc}", status_code=500)

    if output_format == "jsonl":
        def generate_jsonl():
            for row in rows:
                yield json.dumps(row, default=str) + "\n"

        return StreamingResponse(
            generate_jsonl(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f"attachment; filename={view_id}.jsonl"},
        )

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
def collection_browser(request: Request, db_name: str, coll_name: str,
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
    htmx = request.headers.get("HX-Request") == "true"

    if not htmx:
        coll = db.collection(coll_name)
        is_edge = coll.properties().get("type") in ("edge", 3)
        active_filter_summary = _build_active_filter_summary(
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            active_filters=active_filters,
            search_term=search_term,
        )
        stats_url = _build_collection_stats_url(db_name, coll_name, active_filters, search_term=search_term)
        facets_url = _build_collection_facets_url(
            db_name=db_name,
            coll_name=coll_name,
            facet_filters=active_filters,
            search_term=search_term,
            page_size=page_size,
        )
        facet_loaders = _build_collection_facet_loaders(
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            category_fields=category_fields,
            active_filters=active_filters,
            search_term=search_term,
        )
        rows_url = _build_collection_url(
            db_name=db_name,
            coll_name=coll_name,
            page=page,
            page_size=page_size,
            facet_filters=active_filters,
            search_term=search_term,
        )
        download_url = _build_collection_download_url(
            db_name=db_name,
            coll_name=coll_name,
            page=page,
            page_size=page_size,
            facet_filters=active_filters,
            search_term=search_term,
        )
        clear_search_url = _build_collection_url(
            db_name=db_name,
            coll_name=coll_name,
            page=1,
            page_size=page_size,
            facet_filters=active_filters,
            search_term="",
        )
        return templates.TemplateResponse(request, "collection.html", {
            "request": request,
            "db_name": db_name,
            "coll_name": coll_name,
            "docs": [],
            "columns": [],
            "is_edge": is_edge,
            "total": None,
            "page": page,
            "page_size": page_size,
            "total_pages": 1,
            "has_facets": bool(category_fields),
            "facet_panels": [],
            "facet_loaders": facet_loaders,
            "active_filters": active_filters,
            "active_filter_summary": active_filter_summary,
            "search_term": search_term,
            "search_fields": search_fields,
            "stats_url": stats_url,
            "facets_url": facets_url,
            "download_url": download_url,
            "clear_search_url": clear_search_url,
            "rows_url": rows_url,
            "loading_table": True,
            "prev_url": None,
            "next_url": None,
        })

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
    preview_fields = _get_collection_preview_fields(
        is_edge=is_edge,
        facet_metadata=facet_metadata,
        search_metadata=search_metadata,
    )
    list_bind_vars["return_fields"] = preview_fields

    # Fetch documents
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
            LIMIT @skip, @top
            RETURN KEEP(doc, @return_fields)
    """
    cursor = db.aql.execute(query, bind_vars=list_bind_vars)
    docs = list(cursor)
    # Discover columns from this page of results
    columns = _discover_columns(docs, is_edge)
    active_filter_summary = _build_active_filter_summary(
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        active_filters=active_filters,
        search_term=search_term,
    )
    stats_url = _build_collection_stats_url(db_name, coll_name, active_filters, search_term=search_term)
    facets_url = _build_collection_facets_url(
        db_name=db_name,
        coll_name=coll_name,
        facet_filters=active_filters,
        search_term=search_term,
        page_size=page_size,
    )
    facet_loaders = _build_collection_facet_loaders(
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        category_fields=category_fields,
        active_filters=active_filters,
        search_term=search_term,
    )
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

    return templates.TemplateResponse(request, "collection_rows.html", {
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
        "has_facets": bool(category_fields),
        "facet_panels": [],
        "facet_loaders": facet_loaders,
        "active_filters": active_filters,
        "active_filter_summary": active_filter_summary,
        "search_term": search_term,
        "search_fields": search_fields,
        "stats_url": stats_url,
        "facets_url": facets_url,
        "download_url": download_url,
        "clear_search_url": clear_search_url,
        "rows_url": "",
        "loading_table": False,
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
    preview_fields = _get_collection_preview_fields(
        is_edge=is_edge,
        facet_metadata=facet_metadata,
        search_metadata=search_metadata,
    )

    # Match the export columns to the current list-page view by rediscovering
    # columns from the currently visible page, then export all filtered rows.
    page = max(page, 1)
    page_size = max(page_size, 1)
    skip = (page - 1) * page_size
    preview_bind_vars = {**filter_bind_vars, "skip": skip, "top": page_size, "return_fields": preview_fields}
    preview_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
            LIMIT @skip, @top
            RETURN KEEP(doc, @return_fields)
    """
    preview_docs = list(db.aql.execute(preview_query, bind_vars=preview_bind_vars))
    columns = _discover_columns(preview_docs, is_edge)
    if not columns:
        columns = ["_key"]

    export_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
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


@app.get("/db/{db_name}/collection/{coll_name}/facets", response_class=HTMLResponse)
def collection_facets(request: Request, db_name: str, coll_name: str, page_size: int = 25):
    """Facet panels for a collection (loaded after the main table)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
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
    return templates.TemplateResponse(request, "collection_facets.html", {
        "request": request,
        "facet_panels": facet_panels,
    })


@app.get("/db/{db_name}/collection/{coll_name}/facet/{field}", response_class=HTMLResponse)
def collection_facet(request: Request, db_name: str, coll_name: str, field: str, page_size: int = 25):
    """Single facet panel for a collection (loaded independently via HTMX)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    if field not in category_fields:
        return templates.TemplateResponse(request, "collection_facet_panel.html", {
            "request": request,
            "panel": {
                "field": field,
                "title": field.replace("_", " "),
                "values": [],
                "selected_values": [],
                "clear_href": "#",
            },
        })
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    panel = _build_collection_facet_panel(
        db=db,
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        field=field,
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
    )
    return templates.TemplateResponse(request, "collection_facet_panel.html", {
        "request": request,
        "panel": panel,
    })


@app.get("/db/{db_name}/collection/{coll_name}/stats", response_class=HTMLResponse)
def collection_stats(request: Request, db_name: str, coll_name: str):
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
    scalar_fields, list_fields, nested_fields = _categorize_document_fields(doc)

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

    template_name = _get_document_template(db_name, coll_name)

    context = {
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
    }
    if template_name == "cure_case_report_document.html":
        context.update(_get_adjacent_collection_docs(db, coll_name, doc))
        context.update(_build_cure_case_report_context(db_name, db, doc))
    return templates.TemplateResponse(request, template_name, context)


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


def _categorize_document_fields(doc: dict | None):
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

    return scalar_fields, list_fields, nested_fields


def _get_adjacent_collection_docs(db, coll_name: str, doc: dict | None) -> dict:
    if not doc:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": None}

    current_key = doc.get("_key")
    if not current_key:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": "missing _key"}

    try:
        cursor = db.aql.execute(
            f"""
            LET previous_doc = FIRST(
                FOR d IN `{coll_name}`
                FILTER d._key < @current_key
                SORT d._key DESC
                LIMIT 1
                RETURN KEEP(d, "_key", "id", "name")
            )
            LET next_doc = FIRST(
                FOR d IN `{coll_name}`
                FILTER d._key > @current_key
                SORT d._key ASC
                LIMIT 1
                RETURN KEEP(d, "_key", "id", "name")
            )
            RETURN {{
                previous_doc: previous_doc,
                next_doc: next_doc
            }}
            """,
            bind_vars={"current_key": current_key},
            max_runtime=10,
        )
        result = next(iter(cursor), {"previous_doc": None, "next_doc": None})
        result["adjacent_error"] = None
        return result
    except Exception as exc:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": str(exc)}


def _build_cure_case_report_context(db_name: str, db, doc: dict | None) -> dict:
    if not doc or not db.has_graph("graph"):
        return {"cure_case_url": _build_cure_case_url(db_name, doc)}

    try:
        cursor = db.aql.execute(
            """
            FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(v._id, '/')[0] == 'Patient'
            FILTER SPLIT(e._id, '/')[0] == 'CaseReportPatientEdge'
            LIMIT 1
            RETURN v
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        patient_doc = next(iter(cursor), None)
    except Exception:
        patient_doc = None

    try:
        reporter_cursor = db.aql.execute(
            """
            FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(v._id, '/')[0] == 'Reporter'
            FILTER SPLIT(e._id, '/')[0] == 'CaseReportReporterEdge'
            LIMIT 1
            RETURN v
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        reporter_doc = next(iter(reporter_cursor), None)
    except Exception:
        reporter_doc = None

    try:
        clinical_context_cursor = db.aql.execute(
            """
            LET via_patient = (
              FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                FILTER SPLIT(patient._id, '/')[0] == 'Patient'
                FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
                FOR clinical_context, clinical_context_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
                  FILTER SPLIT(clinical_context._id, '/')[0] == 'ClinicalContext'
                  FILTER SPLIT(clinical_context_edge._id, '/')[0] == 'PatientClinicalContextEdge'
                  LIMIT 1
                  RETURN clinical_context
            )
            LET via_report = (
              FOR clinical_context, clinical_context_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                FILTER SPLIT(clinical_context._id, '/')[0] == 'ClinicalContext'
                FILTER SPLIT(clinical_context_edge._id, '/')[0] == 'CaseReportClinicalContextEdge'
                LIMIT 1
                RETURN clinical_context
            )
            RETURN FIRST(APPEND(via_patient, via_report))
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        clinical_context_doc = next(iter(clinical_context_cursor), None)
    except Exception:
        clinical_context_doc = None

    try:
        primary_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'primary'
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        primary_episode_doc = next(iter(primary_episode_cursor), None)
    except Exception:
        primary_episode_doc = None

    try:
        acute_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'contextual'
            FILTER LENGTH(
                FOR condition, condition_edge IN 1..1 OUTBOUND episode._id GRAPH 'graph'
                  FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                  FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                  FILTER condition.slug == 'acute-covid-19'
                  LIMIT 1
                  RETURN 1
            ) > 0
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        acute_episode_doc = next(iter(acute_episode_cursor), None)
    except Exception:
        acute_episode_doc = None

    try:
        pregnancy_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'contextual'
            FILTER LENGTH(
                FOR condition, condition_edge IN 1..1 OUTBOUND episode._id GRAPH 'graph'
                  FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                  FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                  FILTER condition.slug == 'pregnancy'
                  LIMIT 1
                  RETURN 1
            ) > 0
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        pregnancy_episode_doc = next(iter(pregnancy_episode_cursor), None)
    except Exception:
        pregnancy_episode_doc = None

    patient_scalar_fields, patient_list_fields, patient_nested_fields = _categorize_document_fields(patient_doc)
    reporter_scalar_fields, reporter_list_fields, reporter_nested_fields = _categorize_document_fields(reporter_doc)
    clinical_context_scalar_fields, clinical_context_list_fields, clinical_context_nested_fields = _categorize_document_fields(clinical_context_doc)
    primary_episode_scalar_fields, primary_episode_list_fields, primary_episode_nested_fields = _categorize_document_fields(primary_episode_doc)
    acute_episode_scalar_fields, acute_episode_list_fields, acute_episode_nested_fields = _categorize_document_fields(acute_episode_doc)
    pregnancy_episode_scalar_fields, pregnancy_episode_list_fields, pregnancy_episode_nested_fields = _categorize_document_fields(pregnancy_episode_doc)

    episode_relationship_cards = []
    clinical_context_condition_cards = []
    clinical_context_phenotype_cards = []
    background_context_doc = None
    background_context_scalar_fields = []
    background_context_list_fields = []
    background_context_nested_fields = []
    perinatal_context_doc = None
    perinatal_context_scalar_fields = []
    perinatal_context_list_fields = []
    perinatal_context_nested_fields = []
    perinatal_context_phenotype_cards = []
    rasopathies_diagnosis_cards = []
    rasopathies_drug_treatment_cards = []
    patient_prior_condition_cards = []
    background_regular_medicine_cards = []
    background_immunosuppressant_cards = []
    primary_episode_post_covid_condition_cards = []
    exposure_cards = []
    acute_exposure_cards = []
    pregnancy_exposure_cards = []
    acute_complication_cards = []
    acute_vaccination_card = None
    phenotype_cards = []
    therapy_cards = []
    treatment_cards = []
    outcome_cards = []

    if clinical_context_doc:
        try:
            clinical_context_condition_cursor = db.aql.execute(
                """
                FOR condition, condition_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                FILTER SPLIT(condition_edge._id, '/')[0] == 'ClinicalContextConditionEdge'
                SORT condition.name, condition._key
                RETURN {
                    condition: condition,
                    condition_edge: condition_edge
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in clinical_context_condition_cursor:
                condition_doc = row.get("condition")
                condition_edge_doc = row.get("condition_edge")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                clinical_context_condition_cards.append({
                    "condition_doc": condition_doc,
                    "condition_edge_doc": condition_edge_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            clinical_context_condition_cards = []

        try:
            clinical_context_phenotype_cursor = db.aql.execute(
                """
                LET via_finding = (
                  FOR finding, finding_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                    FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                    FILTER SPLIT(finding_edge._id, '/')[0] == 'ClinicalContextFindingEdge'
                    FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding._id GRAPH 'graph'
                      FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                      FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                      RETURN {
                        phenotype: phenotype,
                        finding: finding,
                        finding_edge: finding_edge,
                        phenotype_edge: phenotype_edge,
                        sort_group: finding.group,
                        sort_name: phenotype.name
                      }
                )
                LET direct = (
                  FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge._id, '/')[0] == 'ClinicalContextPhenotypeEdge'
                    RETURN {
                      phenotype: phenotype,
                      finding: null,
                      finding_edge: null,
                      phenotype_edge: phenotype_edge,
                      sort_group: phenotype_edge.group,
                      sort_name: phenotype.name
                    }
                )
                FOR row IN (LENGTH(via_finding) > 0 ? via_finding : direct)
                  SORT row.sort_group, row.sort_name, row.phenotype._key
                  RETURN row
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in clinical_context_phenotype_cursor:
                phenotype_doc = row.get("phenotype")
                finding_doc = row.get("finding")
                finding_edge_doc = row.get("finding_edge")
                phenotype_edge_doc = row.get("phenotype_edge")
                display_edge_doc = finding_doc or phenotype_edge_doc
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                edge_scalar_fields, edge_list_fields, edge_nested_fields = _categorize_document_fields(display_edge_doc)
                clinical_context_phenotype_cards.append({
                    "phenotype_doc": phenotype_doc,
                    "finding_doc": finding_doc,
                    "finding_edge_doc": finding_edge_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                    "edge_doc": display_edge_doc,
                    "phenotype_edge_doc": phenotype_edge_doc,
                    "edge_scalar_fields": edge_scalar_fields,
                    "edge_list_fields": edge_list_fields,
                    "edge_nested_fields": edge_nested_fields,
                })
        except Exception:
            clinical_context_phenotype_cards = []

        try:
            diagnosis_cursor = db.aql.execute(
                """
                FOR diagnosis, diagnosis_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(diagnosis._id, '/')[0] == 'Diagnosis'
                FILTER SPLIT(diagnosis_edge._id, '/')[0] == 'ClinicalContextDiagnosisEdge'
                LET conditions = (
                  FOR condition, condition_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                    FILTER SPLIT(condition_edge._id, '/')[0] == 'DiagnosisConditionEdge'
                    SORT condition.name, condition._key
                    RETURN {
                      condition: condition,
                      condition_edge: condition_edge
                    }
                )
                LET genes = (
                  FOR gene, gene_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(gene._id, '/')[0] == 'Gene'
                    FILTER SPLIT(gene_edge._id, '/')[0] == 'DiagnosisGeneEdge'
                    SORT gene.symbol, gene.name, gene._key
                    RETURN {
                      gene: gene,
                      gene_edge: gene_edge
                    }
                )
                LET variants = (
                  FOR variant, variant_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(variant._id, '/')[0] == 'GeneVariant'
                    FILTER SPLIT(variant_edge._id, '/')[0] == 'DiagnosisGeneVariantEdge'
                    LET linked_genes = (
                      FOR gene, gene_variant_edge IN 1..1 INBOUND variant._id GRAPH 'graph'
                        FILTER SPLIT(gene._id, '/')[0] == 'Gene'
                        FILTER SPLIT(gene_variant_edge._id, '/')[0] == 'GeneGeneVariantEdge'
                        SORT gene.symbol, gene.name, gene._key
                        RETURN {
                          gene: gene,
                          gene_variant_edge: gene_variant_edge
                        }
                    )
                    SORT variant.source_gene_symbol, variant.variant_label, variant._key
                    RETURN {
                      variant: variant,
                      variant_edge: variant_edge,
                      linked_genes: linked_genes
                    }
                )
                SORT diagnosis._key
                RETURN {
                  diagnosis: diagnosis,
                  diagnosis_edge: diagnosis_edge,
                  conditions: conditions,
                  genes: genes,
                  variants: variants
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in diagnosis_cursor:
                diagnosis_doc = row.get("diagnosis")
                diagnosis_scalar_fields, diagnosis_list_fields, diagnosis_nested_fields = _categorize_document_fields(diagnosis_doc)
                condition_cards = []
                for condition_row in row.get("conditions") or []:
                    condition_doc = condition_row.get("condition")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                    condition_cards.append({
                        "condition_doc": condition_doc,
                        "condition_edge_doc": condition_row.get("condition_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                gene_cards = []
                for gene_row in row.get("genes") or []:
                    gene_doc = gene_row.get("gene")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(gene_doc)
                    gene_cards.append({
                        "gene_doc": gene_doc,
                        "gene_edge_doc": gene_row.get("gene_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                variant_cards = []
                for variant_row in row.get("variants") or []:
                    variant_doc = variant_row.get("variant")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(variant_doc)
                    linked_gene_cards = []
                    for linked_gene_row in variant_row.get("linked_genes") or []:
                        gene_doc = linked_gene_row.get("gene")
                        linked_gene_cards.append({
                            "gene_doc": gene_doc,
                            "gene_variant_edge_doc": linked_gene_row.get("gene_variant_edge"),
                        })
                    variant_cards.append({
                        "variant_doc": variant_doc,
                        "variant_edge_doc": variant_row.get("variant_edge"),
                        "linked_gene_cards": linked_gene_cards,
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                rasopathies_diagnosis_cards.append({
                    "diagnosis_doc": diagnosis_doc,
                    "diagnosis_edge_doc": row.get("diagnosis_edge"),
                    "diagnosis_scalar_fields": diagnosis_scalar_fields,
                    "diagnosis_list_fields": diagnosis_list_fields,
                    "diagnosis_nested_fields": diagnosis_nested_fields,
                    "condition_cards": condition_cards,
                    "gene_cards": gene_cards,
                    "variant_cards": variant_cards,
                })
        except Exception:
            rasopathies_diagnosis_cards = []

    finding_group_order = [
        "Cardiac",
        "Endocrine/Growth",
        "Gastrointestinal",
        "Hematologic/Oncologic",
        "Lymphatic/Immunologic",
        "Neurologic/Audiologic",
        "Opthalmalogic",
        "Diagnoses not listed above",
    ]
    finding_group_rank = {name: index for index, name in enumerate(finding_group_order)}
    grouped_clinical_context_phenotype_cards = []
    if clinical_context_phenotype_cards:
        grouped = {}
        for card in clinical_context_phenotype_cards:
            group_name = ((card.get("edge_doc") or {}).get("group") or "Ungrouped").strip()
            grouped.setdefault(group_name, []).append(card)
        for group_name, cards in grouped.items():
            cards.sort(
                key=lambda card: (
                    ((card.get("phenotype_doc") or {}).get("name") or "").lower(),
                    ((card.get("phenotype_doc") or {}).get("_key") or ""),
                )
            )
        grouped_clinical_context_phenotype_cards = [
            {
                "group_name": group_name,
                "cards": grouped[group_name],
            }
            for group_name in sorted(
                grouped.keys(),
                key=lambda name: (
                    finding_group_rank.get(name, 10_000),
                    name == "Ungrouped",
                    name.lower(),
                ),
            )
        ]

    if primary_episode_doc:
        if acute_episode_doc:
            try:
                episode_relationship_cursor = db.aql.execute(
                    """
                    FOR episode, episode_edge IN 1..1 OUTBOUND @acute_episode_id GRAPH 'graph'
                    FILTER SPLIT(episode._id, '/')[0] == 'Episode'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeEpisodeEdge'
                    FILTER episode_edge.relationship_type == 'precedes'
                    FILTER episode._id == @primary_episode_id
                    LIMIT 1
                    RETURN episode_edge
                    """,
                    bind_vars={
                        "acute_episode_id": acute_episode_doc["_id"],
                        "primary_episode_id": primary_episode_doc["_id"],
                    },
                    max_runtime=10,
                )
                relationship_doc = next(iter(episode_relationship_cursor), None)
                if relationship_doc:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(relationship_doc)
                    episode_relationship_cards.append({
                        "relationship_doc": relationship_doc,
                        "label": "precedes",
                        "left_tab": "acute-covid",
                        "right_tab": "long-covid",
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pass
        if pregnancy_episode_doc:
            try:
                overlap_relationship_cursor = db.aql.execute(
                    """
                    FOR episode, episode_edge IN 1..1 OUTBOUND @pregnancy_episode_id GRAPH 'graph'
                    FILTER SPLIT(episode._id, '/')[0] == 'Episode'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeEpisodeEdge'
                    FILTER episode_edge.relationship_type == 'overlaps'
                    FILTER episode._id == @primary_episode_id
                    LIMIT 1
                    RETURN episode_edge
                    """,
                    bind_vars={
                        "pregnancy_episode_id": pregnancy_episode_doc["_id"],
                        "primary_episode_id": primary_episode_doc["_id"],
                    },
                    max_runtime=10,
                )
                relationship_doc = next(iter(overlap_relationship_cursor), None)
                if relationship_doc:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(relationship_doc)
                    episode_relationship_cards.append({
                        "relationship_doc": relationship_doc,
                        "label": "overlaps",
                        "left_tab": "long-covid",
                        "right_tab": "pregnancy",
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pass

        if patient_doc:
            try:
                background_context_cursor = db.aql.execute(
                    """
                    FOR background_context, context_edge IN 1..1 OUTBOUND @person_id GRAPH 'graph'
                    FILTER SPLIT(background_context._id, '/')[0] == 'BackgroundContext'
                    FILTER SPLIT(context_edge._id, '/')[0] == 'PersonBackgroundContextEdge'
                    LIMIT 1
                    RETURN background_context
                    """,
                    bind_vars={"person_id": patient_doc["_id"]},
                    max_runtime=15,
                )
                background_context_doc = next(iter(background_context_cursor), None)
            except Exception:
                background_context_doc = None

            background_context_scalar_fields, background_context_list_fields, background_context_nested_fields = _categorize_document_fields(background_context_doc)

            if background_context_doc:
                try:
                    patient_condition_cursor = db.aql.execute(
                        """
                        FOR condition, condition_edge IN 1..1 OUTBOUND @background_context_id GRAPH 'graph'
                        FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                        FILTER SPLIT(condition_edge._id, '/')[0] == 'BackgroundContextConditionEdge'
                        FILTER condition_edge.relationship_type == 'prior_comorbidity'
                        SORT condition.name, condition._key
                        RETURN condition
                        """,
                        bind_vars={"background_context_id": background_context_doc["_id"]},
                        max_runtime=15,
                    )
                    for condition_doc in patient_condition_cursor:
                        scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                        patient_prior_condition_cards.append({
                            "condition_doc": condition_doc,
                            "scalar_fields": scalar_fields,
                            "list_fields": list_fields,
                            "nested_fields": nested_fields,
                        })
                except Exception:
                    patient_prior_condition_cards = []

                try:
                    background_exposure_cursor = db.aql.execute(
                        """
                        FOR exposure, context_edge IN 1..1 OUTBOUND @background_context_id GRAPH 'graph'
                        FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                        FILTER SPLIT(context_edge._id, '/')[0] == 'BackgroundContextExposureEdge'
                        LET drug_doc = FIRST(
                            FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                            FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                            FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                            LIMIT 1
                            RETURN drug
                        )
                        SORT context_edge.relationship_type, drug_doc.name, exposure.long_drug_name, exposure._key
                        RETURN {
                            exposure_doc: exposure,
                            drug_doc: drug_doc,
                            relationship_type: context_edge.relationship_type
                        }
                        """,
                        bind_vars={"background_context_id": background_context_doc["_id"]},
                        max_runtime=15,
                    )
                    for row in background_exposure_cursor:
                        exposure_doc = row.get("exposure_doc")
                        scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                        card = {
                            "exposure_doc": exposure_doc,
                            "drug_doc": row.get("drug_doc"),
                            "scalar_fields": scalar_fields,
                            "list_fields": list_fields,
                            "nested_fields": nested_fields,
                        }
                        if row.get("relationship_type") == "immunosuppressant":
                            background_immunosuppressant_cards.append(card)
                        else:
                            background_regular_medicine_cards.append(card)
                except Exception:
                    background_regular_medicine_cards = []
                    background_immunosuppressant_cards = []

    if patient_doc:
        try:
            perinatal_context_cursor = db.aql.execute(
                """
                FOR perinatal_context, context_edge IN 1..1 OUTBOUND @person_id GRAPH 'graph'
                FILTER SPLIT(perinatal_context._id, '/')[0] == 'PerinatalContext'
                FILTER SPLIT(context_edge._id, '/')[0] == 'PatientPerinatalContextEdge'
                LIMIT 1
                RETURN perinatal_context
                """,
                bind_vars={"person_id": patient_doc["_id"]},
                max_runtime=15,
            )
            perinatal_context_doc = next(iter(perinatal_context_cursor), None)
        except Exception:
            perinatal_context_doc = None

        perinatal_context_scalar_fields, perinatal_context_list_fields, perinatal_context_nested_fields = _categorize_document_fields(perinatal_context_doc)
        if perinatal_context_doc:
            try:
                perinatal_phenotype_cursor = db.aql.execute(
                    """
                    LET via_finding = (
                      FOR finding, finding_edge IN 1..1 OUTBOUND @perinatal_context_id GRAPH 'graph'
                        FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                        FILTER SPLIT(finding_edge._id, '/')[0] == 'PerinatalContextFindingEdge'
                        FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding._id GRAPH 'graph'
                          FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                          FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                          RETURN {
                            phenotype: phenotype,
                            finding: finding,
                            finding_edge: finding_edge,
                            phenotype_edge: phenotype_edge,
                            sort_name: phenotype.name
                          }
                    )
                    LET direct = (
                      FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @perinatal_context_id GRAPH 'graph'
                        FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                        FILTER SPLIT(phenotype_edge._id, '/')[0] == 'PerinatalContextPhenotypeEdge'
                        RETURN {
                          phenotype: phenotype,
                          finding: null,
                          finding_edge: null,
                          phenotype_edge: phenotype_edge,
                          sort_name: phenotype.name
                        }
                    )
                    FOR row IN (LENGTH(via_finding) > 0 ? via_finding : direct)
                      SORT row.sort_name, row.phenotype._key
                      RETURN row
                    """,
                    bind_vars={"perinatal_context_id": perinatal_context_doc["_id"]},
                    max_runtime=15,
                )
                for row in perinatal_phenotype_cursor:
                    phenotype_doc = row.get("phenotype")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                    perinatal_context_phenotype_cards.append({
                        "phenotype_doc": phenotype_doc,
                        "finding_doc": row.get("finding"),
                        "finding_edge_doc": row.get("finding_edge"),
                        "edge_doc": row.get("finding") or row.get("phenotype_edge"),
                        "phenotype_edge_doc": row.get("phenotype_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                perinatal_context_phenotype_cards = []

        try:
            rasopathies_treatment_cursor = db.aql.execute(
                """
                FOR treatment, treatment_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(treatment._id, '/')[0] == 'DrugTreatment'
                FILTER SPLIT(treatment_edge._id, '/')[0] == 'ClinicalContextDrugTreatmentEdge'
                LET drug_doc = FIRST(
                  FOR drug, drug_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                    FILTER SPLIT(drug_edge._id, '/')[0] == 'DrugTreatmentDrugEdge'
                    LIMIT 1
                    RETURN drug
                )
                LET responses = (
                  FOR response, response_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(response._id, '/')[0] == 'TreatmentResponse'
                    FILTER SPLIT(response_edge._id, '/')[0] == 'DrugTreatmentResponseEdge'
                    LET finding_doc = FIRST(
                      FOR finding, finding_edge IN 1..1 OUTBOUND response._id GRAPH 'graph'
                        FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                        FILTER SPLIT(finding_edge._id, '/')[0] == 'TreatmentResponseFindingEdge'
                        LIMIT 1
                        RETURN finding
                    )
                    LET phenotype_docs = (
                      FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding_doc._id GRAPH 'graph'
                        FILTER finding_doc != null
                        FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                        FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                        SORT phenotype.name, phenotype._key
                        RETURN {
                          phenotype: phenotype,
                          phenotype_edge: phenotype_edge
                        }
                    )
                    SORT response.source_target_index, response._key
                    RETURN {
                      response_doc: response,
                      response_edge_doc: response_edge,
                      finding_doc: finding_doc,
                      phenotype_docs: phenotype_docs
                    }
                )
                LET adverse_events = (
                  FOR phenotype, adverse_event_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(adverse_event_edge._id, '/')[0] == 'DrugTreatmentAdverseEventEdge'
                    SORT adverse_event_edge.source_adverse_event_index, phenotype.name, phenotype._key
                    RETURN {
                      phenotype: phenotype,
                      adverse_event_edge: adverse_event_edge
                    }
                )
                SORT treatment.source_treatment_index, drug_doc.name, treatment._key
                RETURN {
                  treatment_doc: treatment,
                  treatment_edge_doc: treatment_edge,
                  drug_doc: drug_doc,
                  responses: responses,
                  adverse_events: adverse_events
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=20,
            )
            for row in rasopathies_treatment_cursor:
                treatment_doc = row.get("treatment_doc")
                treatment_scalar_fields, treatment_list_fields, treatment_nested_fields = _categorize_document_fields(treatment_doc)
                drug_doc = row.get("drug_doc")
                drug_scalar_fields, drug_list_fields, drug_nested_fields = _categorize_document_fields(drug_doc)
                response_cards = []
                for response_row in row.get("responses") or []:
                    response_doc = response_row.get("response_doc")
                    response_scalar_fields, response_list_fields, response_nested_fields = _categorize_document_fields(response_doc)
                    finding_doc = response_row.get("finding_doc")
                    finding_scalar_fields, finding_list_fields, finding_nested_fields = _categorize_document_fields(finding_doc)
                    phenotype_cards = []
                    for phenotype_row in response_row.get("phenotype_docs") or []:
                        phenotype_doc = phenotype_row.get("phenotype")
                        phenotype_scalar_fields, phenotype_list_fields, phenotype_nested_fields = _categorize_document_fields(phenotype_doc)
                        phenotype_cards.append({
                            "phenotype_doc": phenotype_doc,
                            "phenotype_edge_doc": phenotype_row.get("phenotype_edge"),
                            "scalar_fields": phenotype_scalar_fields,
                            "list_fields": phenotype_list_fields,
                            "nested_fields": phenotype_nested_fields,
                        })
                    response_cards.append({
                        "response_doc": response_doc,
                        "response_edge_doc": response_row.get("response_edge_doc"),
                        "scalar_fields": response_scalar_fields,
                        "list_fields": response_list_fields,
                        "nested_fields": response_nested_fields,
                        "finding_doc": finding_doc,
                        "finding_scalar_fields": finding_scalar_fields,
                        "finding_list_fields": finding_list_fields,
                        "finding_nested_fields": finding_nested_fields,
                        "phenotype_cards": phenotype_cards,
                    })
                adverse_event_cards = []
                for adverse_event_row in row.get("adverse_events") or []:
                    adverse_event_doc = adverse_event_row.get("phenotype")
                    adverse_event_edge_doc = adverse_event_row.get("adverse_event_edge")
                    adverse_event_scalar_fields, adverse_event_list_fields, adverse_event_nested_fields = _categorize_document_fields(adverse_event_doc)
                    adverse_event_edge_scalar_fields, adverse_event_edge_list_fields, adverse_event_edge_nested_fields = _categorize_document_fields(adverse_event_edge_doc)
                    adverse_event_cards.append({
                        "phenotype_doc": adverse_event_doc,
                        "adverse_event_edge_doc": adverse_event_edge_doc,
                        "scalar_fields": adverse_event_scalar_fields,
                        "list_fields": adverse_event_list_fields,
                        "nested_fields": adverse_event_nested_fields,
                        "edge_scalar_fields": adverse_event_edge_scalar_fields,
                        "edge_list_fields": adverse_event_edge_list_fields,
                        "edge_nested_fields": adverse_event_edge_nested_fields,
                    })
                rasopathies_drug_treatment_cards.append({
                    "treatment_doc": treatment_doc,
                    "treatment_edge_doc": row.get("treatment_edge_doc"),
                    "drug_doc": drug_doc,
                    "treatment_scalar_fields": treatment_scalar_fields,
                    "treatment_list_fields": treatment_list_fields,
                    "treatment_nested_fields": treatment_nested_fields,
                    "drug_scalar_fields": drug_scalar_fields,
                    "drug_list_fields": drug_list_fields,
                    "drug_nested_fields": drug_nested_fields,
                    "response_cards": response_cards,
                    "adverse_event_cards": adverse_event_cards,
                })
        except Exception:
            rasopathies_drug_treatment_cards = []

        try:
            primary_post_covid_condition_cursor = db.aql.execute(
                """
                FOR condition, condition_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                FILTER condition_edge.relationship_type == 'comorbidity'
                SORT condition.name, condition._key
                RETURN condition
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for condition_doc in primary_post_covid_condition_cursor:
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                primary_episode_post_covid_condition_cards.append({
                    "condition_doc": condition_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            primary_episode_post_covid_condition_cards = []

        try:
            exposure_cursor = db.aql.execute(
                """
                FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                LET drug_doc = FIRST(
                    FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                    FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                    FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                    LIMIT 1
                    RETURN drug
                )
                LET adverse_events = (
                    FOR ae, ae_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                    FILTER SPLIT(ae._id, '/')[0] IN ['Phenotype', 'AdverseEvent']
                    FILTER SPLIT(ae_edge._id, '/')[0] == 'ExposureAdverseEventEdge'
                    SORT ae.name
                    RETURN {
                        id: ae._id,
                        key: ae._key,
                        collection: SPLIT(ae._id, '/')[0],
                        name: ae.name,
                        outcomes: ae_edge.outcomes || []
                    }
                )
                SORT exposure.long_drug_name, exposure._key
                RETURN {
                    exposure_doc: exposure,
                    drug_doc: drug_doc,
                    adverse_events: adverse_events
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in exposure_cursor:
                exposure_doc = row.get("exposure_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                exposure_cards.append({
                    "exposure_doc": exposure_doc,
                    "drug_doc": row.get("drug_doc"),
                    "adverse_events": row.get("adverse_events", []),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            exposure_cards = []

        if acute_episode_doc:
            try:
                acute_exposure_cursor = db.aql.execute(
                    """
                    FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT exposure.long_drug_name, exposure._key
                    RETURN {
                        exposure_doc: exposure,
                        drug_doc: drug_doc,
                        adverse_events: []
                    }
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                for row in acute_exposure_cursor:
                    exposure_doc = row.get("exposure_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                    acute_exposure_cards.append({
                        "exposure_doc": exposure_doc,
                        "drug_doc": row.get("drug_doc"),
                        "adverse_events": [],
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                acute_exposure_cards = []

            try:
                acute_complication_cursor = db.aql.execute(
                    """
                    FOR condition, complication_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                    FILTER SPLIT(complication_edge._id, '/')[0] == 'EpisodeConditionEdge'
                    FILTER complication_edge.relationship_type == 'complication'
                    SORT condition.name, condition._key
                    RETURN condition
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                for condition_doc in acute_complication_cursor:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                    acute_complication_cards.append({
                        "condition_doc": condition_doc,
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                acute_complication_cards = []

            try:
                acute_vaccination_cursor = db.aql.execute(
                    """
                    FOR vaccination_event, event_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(vaccination_event._id, '/')[0] == 'VaccinationEvent'
                    FILTER SPLIT(event_edge._id, '/')[0] == 'VaccinationEventEpisodeEdge'
                    LET vaccines = (
                        FOR vaccine, vaccine_edge IN 1..1 OUTBOUND vaccination_event._id GRAPH 'graph'
                        FILTER SPLIT(vaccine._id, '/')[0] == 'Vaccine'
                        FILTER SPLIT(vaccine_edge._id, '/')[0] == 'VaccinationEventVaccineEdge'
                        SORT vaccine.name, vaccine._key
                        RETURN vaccine
                    )
                    LIMIT 1
                    RETURN {
                        vaccination_event_doc: vaccination_event,
                        event_edge: event_edge,
                        vaccines: vaccines
                    }
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                acute_vaccination_row = next(iter(acute_vaccination_cursor), None)
                if acute_vaccination_row:
                    vaccination_event_doc = acute_vaccination_row.get("vaccination_event_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(vaccination_event_doc)
                    acute_vaccination_card = {
                        "vaccination_event_doc": vaccination_event_doc,
                        "event_edge": acute_vaccination_row.get("event_edge") or {},
                        "vaccines": acute_vaccination_row.get("vaccines", []),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    }
            except Exception:
                acute_vaccination_card = None

        if pregnancy_episode_doc:
            try:
                pregnancy_exposure_cursor = db.aql.execute(
                    """
                    FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT exposure.long_drug_name, exposure._key
                    RETURN {
                        exposure_doc: exposure,
                        drug_doc: drug_doc,
                        adverse_events: []
                    }
                    """,
                    bind_vars={"episode_id": pregnancy_episode_doc["_id"]},
                    max_runtime=15,
                )
                for row in pregnancy_exposure_cursor:
                    exposure_doc = row.get("exposure_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                    pregnancy_exposure_cards.append({
                        "exposure_doc": exposure_doc,
                        "drug_doc": row.get("drug_doc"),
                        "adverse_events": [],
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pregnancy_exposure_cards = []

        try:
            phenotype_cursor = db.aql.execute(
                """
                FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                FILTER SPLIT(phenotype_edge._id, '/')[0] == 'EpisodePhenotypeEdge'
                SORT phenotype.name, phenotype._key
                RETURN {
                    phenotype_doc: phenotype,
                    severity: phenotype_edge.severity
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in phenotype_cursor:
                phenotype_doc = row.get("phenotype_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                phenotype_cards.append({
                    "phenotype_doc": phenotype_doc,
                    "severity": row.get("severity"),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            phenotype_cards = []

        try:
            therapy_cursor = db.aql.execute(
                """
                FOR therapy, therapy_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(therapy._id, '/')[0] == 'Therapy'
                FILTER SPLIT(therapy_edge._id, '/')[0] == 'EpisodeTherapyEdge'
                SORT therapy.name, therapy._key
                RETURN therapy
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for therapy_doc in therapy_cursor:
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(therapy_doc)
                therapy_cards.append({
                    "therapy_doc": therapy_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            therapy_cards = []

        try:
            treatment_cursor = db.aql.execute(
                """
                FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                FOR treatment, treatment_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                FILTER SPLIT(treatment._id, '/')[0] == 'Treatment'
                FILTER SPLIT(treatment_edge._id, '/')[0] == 'TreatmentExposureEdge'
                COLLECT treatment_id = treatment._id INTO grouped = {
                    treatment: treatment,
                    exposure: exposure
                }
                LET treatment_doc = FIRST(grouped[*].treatment)
                LET mapped_exposures = (
                    FOR row IN grouped
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND row.exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT drug_doc.name, row.exposure.long_drug_name, row.exposure._key
                    RETURN {
                        exposure_id: row.exposure._id,
                        drug_name: drug_doc.name || row.exposure.long_drug_name || row.exposure._key
                    }
                )
                SORT treatment_doc._key
                RETURN {
                    treatment_doc: treatment_doc,
                    mapped_exposures: mapped_exposures
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in treatment_cursor:
                treatment_doc = row.get("treatment_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(treatment_doc)
                treatment_cards.append({
                    "treatment_doc": treatment_doc,
                    "mapped_exposures": row.get("mapped_exposures", []),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            treatment_cards = []

        try:
            outcome_cursor = db.aql.execute(
                """
                FOR outcome, episode_outcome_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(outcome._id, '/')[0] == 'Outcome'
                FILTER SPLIT(episode_outcome_edge._id, '/')[0] == 'EpisodeOutcomeEdge'
                LET treatment_doc = FIRST(
                    FOR treatment, outcome_edge IN 1..1 INBOUND outcome._id GRAPH 'graph'
                    FILTER SPLIT(treatment._id, '/')[0] == 'Treatment'
                    FILTER SPLIT(outcome_edge._id, '/')[0] == 'TreatmentOutcomeEdge'
                    LIMIT 1
                    RETURN treatment
                )
                LET phenotype_doc = FIRST(
                    FOR phenotype, phenotype_edge IN 1..1 OUTBOUND outcome._id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge._id, '/')[0] == 'OutcomePhenotypeEdge'
                    LIMIT 1
                    RETURN phenotype
                )
                LET phenotype_severity = FIRST(
                    FOR phenotype2, phenotype_edge2 IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(phenotype2._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge2._id, '/')[0] == 'EpisodePhenotypeEdge'
                    FILTER phenotype_doc != null AND phenotype2._id == phenotype_doc._id
                    LIMIT 1
                    RETURN phenotype_edge2.severity
                )
                LET mapped_exposures = (
                    FOR exposure2, exposure_edge2 IN 1..1 INBOUND treatment_doc._id GRAPH 'graph'
                    FILTER SPLIT(exposure2._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(exposure_edge2._id, '/')[0] == 'TreatmentExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure2._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT drug_doc.name, exposure2.long_drug_name, exposure2._key
                    RETURN {
                        exposure_id: exposure2._id,
                        exposure_doc: exposure2,
                        drug_doc: drug_doc,
                        drug_name: drug_doc.name || exposure2.long_drug_name || exposure2._key
                    }
                )
                SORT outcome._key
                RETURN {
                    outcome_doc: outcome,
                    treatment_doc: treatment_doc,
                    phenotype_doc: phenotype_doc,
                    phenotype_severity: phenotype_severity,
                    mapped_exposures: mapped_exposures
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=20,
            )
            for row in outcome_cursor:
                outcome_doc = row.get("outcome_doc")
                effect_display = _get_outcome_effect_display((outcome_doc or {}).get("effect"))
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(outcome_doc)
                phenotype_doc = row.get("phenotype_doc")
                phenotype_scalar_fields, phenotype_list_fields, phenotype_nested_fields = _categorize_document_fields(phenotype_doc)
                treatment_doc = row.get("treatment_doc")
                treatment_scalar_fields, treatment_list_fields, treatment_nested_fields = _categorize_document_fields(treatment_doc)
                outcome_cards.append({
                    "outcome_doc": outcome_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                    "treatment_doc": treatment_doc,
                    "treatment_scalar_fields": treatment_scalar_fields,
                    "treatment_list_fields": treatment_list_fields,
                    "treatment_nested_fields": treatment_nested_fields,
                    "mapped_exposures": row.get("mapped_exposures", []),
                    "phenotype_doc": phenotype_doc,
                    "phenotype_severity": row.get("phenotype_severity"),
                    "phenotype_scalar_fields": phenotype_scalar_fields,
                    "phenotype_list_fields": phenotype_list_fields,
                    "phenotype_nested_fields": phenotype_nested_fields,
                    "effect_rank": effect_display["rank"],
                    "effect_tone": effect_display["tone"],
                    "effect_pct": effect_display["pct"],
                })
            outcome_cards.sort(
                key=lambda card: (
                    card["effect_rank"],
                    (card["outcome_doc"] or {}).get("effect") or "",
                    (card["outcome_doc"] or {}).get("raw_symptom_name") or "",
                    (card["outcome_doc"] or {}).get("_key") or "",
                )
            )
        except Exception:
            outcome_cards = []

    return {
        "cure_case_url": _build_cure_case_url(db_name, doc),
        "patient_doc": patient_doc,
        "patient_scalar_fields": patient_scalar_fields,
        "patient_list_fields": patient_list_fields,
        "patient_nested_fields": patient_nested_fields,
        "reporter_doc": reporter_doc,
        "reporter_scalar_fields": reporter_scalar_fields,
        "reporter_list_fields": reporter_list_fields,
        "reporter_nested_fields": reporter_nested_fields,
        "clinical_context_doc": clinical_context_doc,
        "clinical_context_scalar_fields": clinical_context_scalar_fields,
        "clinical_context_list_fields": clinical_context_list_fields,
        "clinical_context_nested_fields": clinical_context_nested_fields,
        "clinical_context_condition_cards": clinical_context_condition_cards,
        "clinical_context_phenotype_cards": clinical_context_phenotype_cards,
        "grouped_clinical_context_phenotype_cards": grouped_clinical_context_phenotype_cards,
        "background_context_doc": background_context_doc,
        "background_context_scalar_fields": background_context_scalar_fields,
        "background_context_list_fields": background_context_list_fields,
        "background_context_nested_fields": background_context_nested_fields,
        "perinatal_context_doc": perinatal_context_doc,
        "perinatal_context_scalar_fields": perinatal_context_scalar_fields,
        "perinatal_context_list_fields": perinatal_context_list_fields,
        "perinatal_context_nested_fields": perinatal_context_nested_fields,
        "perinatal_context_phenotype_cards": perinatal_context_phenotype_cards,
        "rasopathies_diagnosis_cards": rasopathies_diagnosis_cards,
        "rasopathies_drug_treatment_cards": rasopathies_drug_treatment_cards,
        "episode_relationship_cards": episode_relationship_cards,
        "patient_prior_condition_cards": patient_prior_condition_cards,
        "background_regular_medicine_cards": background_regular_medicine_cards,
        "background_immunosuppressant_cards": background_immunosuppressant_cards,
        "primary_episode_doc": primary_episode_doc,
        "primary_episode_scalar_fields": primary_episode_scalar_fields,
        "primary_episode_list_fields": primary_episode_list_fields,
        "primary_episode_nested_fields": primary_episode_nested_fields,
        "primary_episode_post_covid_condition_cards": primary_episode_post_covid_condition_cards,
        "acute_episode_doc": acute_episode_doc,
        "acute_episode_scalar_fields": acute_episode_scalar_fields,
        "acute_episode_list_fields": acute_episode_list_fields,
        "acute_episode_nested_fields": acute_episode_nested_fields,
        "pregnancy_episode_doc": pregnancy_episode_doc,
        "pregnancy_episode_scalar_fields": pregnancy_episode_scalar_fields,
        "pregnancy_episode_list_fields": pregnancy_episode_list_fields,
        "pregnancy_episode_nested_fields": pregnancy_episode_nested_fields,
        "pregnancy_episode_exposure_cards": pregnancy_exposure_cards,
        "acute_episode_vaccination_card": acute_vaccination_card,
        "acute_episode_exposure_cards": acute_exposure_cards,
        "acute_episode_complication_cards": acute_complication_cards,
        "primary_episode_exposure_cards": exposure_cards,
        "primary_episode_phenotype_cards": phenotype_cards,
        "primary_episode_therapy_cards": therapy_cards,
        "primary_episode_treatment_cards": treatment_cards,
        "primary_episode_outcome_cards": outcome_cards,
    }


def _build_cure_case_url(db_name: str, doc: dict | None) -> str | None:
    if not doc:
        return None
    if doc.get("case_report_url"):
        return doc.get("case_report_url")
    report_id = doc.get("id") or doc.get("_key")
    if not report_id:
        return None

    route_slug = _get_cure_route_slug(db_name, doc.get("form_type"))
    if not route_slug:
        return None
    return f"https://cure.ncats.io/explore/{route_slug}/case-reports/case-details/{report_id}"


def _is_cure_db(db_name: str) -> bool:
    normalized = (db_name or "").strip().lower()
    return normalized == "cure" or normalized.startswith("cure_")


def _get_cure_route_slug(db_name: str, form_type: str | None = None) -> str | None:
    normalized_db_name = (db_name or "").strip().lower()
    if normalized_db_name == "cure" or normalized_db_name.startswith("cure_pasc"):
        return "long-covid"
    if normalized_db_name.startswith("cure_rasopathies"):
        return "rasopathies"

    return {
        "pasc": "long-covid",
        "rasopathies": "rasopathies",
    }.get((form_type or "").strip().lower())


def _get_outcome_effect_display(effect: str | None) -> dict:
    normalized = (effect or "").strip().lower()
    mapping = {
        "complete symptom resolution": {"rank": 0, "tone": "positive", "pct": 100},
        "significant symptom improvement": {"rank": 1, "tone": "positive", "pct": 82},
        "moderate symptom improvement": {"rank": 2, "tone": "positive", "pct": 64},
        "mild symptom improvement": {"rank": 3, "tone": "positive", "pct": 46},
        "symptom was unchanged": {"rank": 4, "tone": "neutral", "pct": 0},
        "unknown": {"rank": 5, "tone": "unknown", "pct": 0},
        "mild symptom worsening": {"rank": 6, "tone": "negative", "pct": 40},
        "moderate symptom worsening": {"rank": 7, "tone": "negative", "pct": 62},
        "significant symptom worsening": {"rank": 8, "tone": "negative", "pct": 84},
    }
    default = {"rank": 9, "tone": "unknown", "pct": 0}
    return mapping.get(normalized, default)


templates.env.filters["truncate_val"] = _truncate


# ── Document template dispatch ───────────────────────────────────────────────

def _get_document_template(db_name: str, coll_name: str) -> str:
    if coll_name == "CaseReport" and _is_cure_db(db_name):
        return "cure_case_report_document.html"
    return "document.html"


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
