import argparse
import json
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import quote as url_quote

import urllib3
import yaml
from arango import ArangoClient
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
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

# -- Global state set at startup --
_client: Optional[ArangoClient] = None
_credentials: dict = {}
_mysql_credentials: dict = {}
_mysql_db_engines: dict = {}
_mysql_inspector_cache: dict = {}   # db_name -> CachableInspector data


def get_client() -> ArangoClient:
    global _client
    if _client is None:
        url = _credentials.get("url", "http://localhost:8529")
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


def get_mysql_engine() -> Optional[Engine]:
    """Get a MySQL engine (no specific database) for listing databases."""
    if not _mysql_credentials:
        return None
    if "_root" not in _mysql_db_engines:
        host = _mysql_credentials.get("url", "localhost")
        port = _mysql_credentials.get("port", 3306)
        user = _mysql_credentials.get("user", "root")
        password = url_quote(_mysql_credentials.get("password", ""), safe="")
        _mysql_db_engines["_root"] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines["_root"]


def get_mysql_db_engine(db_name: str) -> Engine:
    """Get a MySQL engine scoped to a specific database."""
    if db_name not in _mysql_db_engines:
        host = _mysql_credentials.get("url", "localhost")
        port = _mysql_credentials.get("port", 3306)
        user = _mysql_credentials.get("user", "root")
        password = url_quote(_mysql_credentials.get("password", ""), safe="")
        _mysql_db_engines[db_name] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines[db_name]


def get_mysql_inspector(db_name: str):
    """Return cached schema metadata for a database.

    Cached as a plain dict so we don't hold live Inspector objects.
    Call invalidate_mysql_inspector(db_name) to force a refresh.
    """
    if db_name not in _mysql_inspector_cache:
        engine = get_mysql_db_engine(db_name)
        insp = sa_inspect(engine)
        table_names = insp.get_table_names()
        meta = {}
        for tbl in table_names:
            meta[tbl] = {
                "columns": insp.get_columns(tbl),
                "pk": insp.get_pk_constraint(tbl).get("constrained_columns", []),
                "fks": insp.get_foreign_keys(tbl),
            }
        _mysql_inspector_cache[db_name] = meta
    return _mysql_inspector_cache[db_name]


def invalidate_mysql_inspector(db_name: str):
    """Drop the cached schema for a database so the next request re-fetches it."""
    _mysql_inspector_cache.pop(db_name, None)


# ── Jinja2 filters ──────────────────────────────────────────────────────────

def json_pretty(value):
    try:
        return json.dumps(value, indent=2, default=str)
    except Exception:
        return str(value)


templates.env.filters["json_pretty"] = json_pretty


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
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
    mysql_databases = []
    mysql_url = ""
    engine = get_mysql_engine()
    if engine:
        host = _mysql_credentials.get("url", "localhost")
        port = _mysql_credentials.get("port", 3306)
        mysql_url = f"{host}:{port}"
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SHOW DATABASES"))
                system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
                mysql_databases = [row[0] for row in result if row[0] not in system_dbs]
                print(f"MySQL databases: {mysql_databases}")
        except Exception:
            print("Failed to connect to MySQL:", sys.exc_info()[1])
            pass

    return templates.TemplateResponse("home.html", {
        "request": request,
        "databases": arango_databases,
        "arango_url": arango_url,
        "mysql_databases": mysql_databases,
        "mysql_url": mysql_url,
    })


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

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
        "edge_defs": edge_defs,
        "etl_meta": etl_meta,
        "doc_count": sum(c["count"] for c in collections if c["type"] == "document"),
        "edge_count": sum(c["count"] for c in collections if c["type"] == "edge"),
    })


@app.get("/db/{db_name}/collection/{coll_name}", response_class=HTMLResponse)
async def collection_browser(request: Request, db_name: str, coll_name: str,
                              page: int = 1, page_size: int = 25):
    db = get_db(db_name)
    skip = (page - 1) * page_size

    # Use AQL for both count and list so they always agree
    count_cursor = db.aql.execute(
        f"FOR doc IN `{coll_name}` COLLECT WITH COUNT INTO c RETURN c"
    )
    total = list(count_cursor)[0]
    total_pages = max(1, (total + page_size - 1) // page_size)

    coll = db.collection(coll_name)
    is_edge = coll.properties().get("type") in ("edge", 3)

    # Fetch documents
    query = f"FOR doc IN `{coll_name}` LIMIT @skip, @top RETURN doc"
    cursor = db.aql.execute(query, bind_vars={"skip": skip, "top": page_size})
    docs = list(cursor)
    # Discover columns from this page of results
    columns = _discover_columns(docs, is_edge)

    # HTMX partial rendering
    htmx = request.headers.get("HX-Request") == "true"
    template = "collection_rows.html" if htmx else "collection.html"

    return templates.TemplateResponse(template, {
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
    })


@app.get("/db/{db_name}/collection/{coll_name}/stats", response_class=HTMLResponse)
async def collection_stats(request: Request, db_name: str, coll_name: str):
    """Field coverage stats for a collection (loaded via HTMX)."""
    db = get_db(db_name)
    coll = db.collection(coll_name)
    total = coll.count()
    sample_size = min(total, 500)

    # Sample documents to discover fields
    query = f"""
        FOR doc IN `{coll_name}`
            SORT RAND()
            LIMIT {sample_size}
            RETURN ATTRIBUTES(doc)
    """
    cursor = db.aql.execute(query)
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

    return templates.TemplateResponse("stats_partial.html", {
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
            parquet_path = Path(file_ref)
            if not parquet_path.exists():
                error = f"Parquet file not found: {file_ref}"
            else:
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(str(parquet_path))
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
                    "file_path": str(parquet_path),
                    "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
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

    return templates.TemplateResponse("parquet_stats.html", {
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

    return templates.TemplateResponse("document.html", {
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

    return templates.TemplateResponse("schema.html", {
        "request": request,
        "db_name": db_name,
        "edge_defs": edge_defs,
        "mermaid_text": mermaid_text,
    })


@app.get("/db/{db_name}/aql", response_class=HTMLResponse)
async def aql_page(request: Request, db_name: str):
    return templates.TemplateResponse("aql.html", {
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

    return templates.TemplateResponse(template, {
        "request": request,
        "db_name": db_name,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
    })


# ── MySQL Routes ─────────────────────────────────────────────────────────────

@app.get("/mysql/{db_name}", response_class=HTMLResponse)
async def mysql_dashboard(request: Request, db_name: str):
    engine = get_mysql_db_engine(db_name)
    schema_meta = get_mysql_inspector(db_name)

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

    return templates.TemplateResponse("mysql_dashboard.html", {
        "request": request,
        "db_name": db_name,
        "tables": tables,
        "fk_defs": fk_defs,
        "table_count": len(tables),
        "total_rows": sum(t["count"] for t in tables),
    })


@app.get("/mysql/{db_name}/table/{table_name}", response_class=HTMLResponse)
async def mysql_table_browser(request: Request, db_name: str, table_name: str,
                               page: int = 1, page_size: int = 25):
    engine = get_mysql_db_engine(db_name)
    meta = get_mysql_inspector(db_name)[table_name]

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

    return templates.TemplateResponse(template, {
        "request": request,
        "db_name": db_name,
        "table_name": table_name,
        "rows": [dict(r) for r in rows],
        "columns": ordered_columns,
        "pk_cols": pk_cols,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    })


@app.get("/mysql/{db_name}/table/{table_name}/stats", response_class=HTMLResponse)
async def mysql_table_stats(request: Request, db_name: str, table_name: str):
    """Column coverage stats for a MySQL table (loaded via HTMX)."""
    engine = get_mysql_db_engine(db_name)
    columns = get_mysql_inspector(db_name)[table_name]["columns"]

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

    return templates.TemplateResponse("stats_partial.html", {
        "request": request,
        "stats": stats,
        "sample_size": total,
        "total": total,
    })


@app.get("/mysql/{db_name}/table/{table_name}/row/{pk_value:path}", response_class=HTMLResponse)
async def mysql_row_detail(request: Request, db_name: str, table_name: str, pk_value: str):
    engine = get_mysql_db_engine(db_name)
    meta = get_mysql_inspector(db_name)[table_name]
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
                        "url": f"/mysql/{db_name}/table/{fk['referred_table']}/row/{val}",
                    }

    # Get column metadata
    columns_info = {c["name"]: str(c["type"]) for c in meta["columns"]}

    return templates.TemplateResponse("mysql_row.html", {
        "request": request,
        "db_name": db_name,
        "table_name": table_name,
        "row": dict(row) if row else None,
        "pk_value": pk_value,
        "pk_cols": pk_cols,
        "fk_links": fk_links,
        "columns_info": columns_info,
    })


@app.get("/mysql/{db_name}/schema", response_class=HTMLResponse)
async def mysql_schema(request: Request, db_name: str):
    schema_meta = get_mysql_inspector(db_name)

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

    return templates.TemplateResponse("mysql_schema.html", {
        "request": request,
        "db_name": db_name,
        "fk_defs": fk_defs,
        "mermaid_text": mermaid_text,
    })


@app.post("/mysql/{db_name}/refresh-schema", response_class=HTMLResponse)
async def mysql_refresh_schema(request: Request, db_name: str):
    """Bust the schema cache for a database and redirect to dashboard."""
    from fastapi.responses import RedirectResponse
    invalidate_mysql_inspector(db_name)
    return RedirectResponse(url=f"/mysql/{db_name}", status_code=303)


@app.get("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_page(request: Request, db_name: str):
    return templates.TemplateResponse("mysql_sql.html", {
        "request": request,
        "db_name": db_name,
        "results": None,
        "query": "",
        "error": None,
        "columns": [],
    })


@app.post("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_execute(request: Request, db_name: str, query: str = Form(...)):
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
            engine = get_mysql_db_engine(db_name)
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

    return templates.TemplateResponse(template, {
        "request": request,
        "db_name": db_name,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
    })


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


templates.env.filters["truncate_val"] = _truncate


# ── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="QA Browser")
    parser.add_argument("--credentials", "-c",
                        default="./src/use_cases/secrets/local_arangodb.yaml",
                        help="Path to ArangoDB credentials YAML file")
    parser.add_argument("--mysql-credentials", "-m",
                        default=None,
                        help="Path to MySQL credentials YAML file (url, user, password, port)")
    parser.add_argument("--port", "-p", type=int, default=8050)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    global _credentials, _mysql_credentials
    cred_path = Path(args.credentials)
    if cred_path.exists():
        with open(cred_path) as f:
            _credentials = yaml.safe_load(f)
        print(f"Loaded ArangoDB credentials from {cred_path}")
    else:
        print(f"Warning: {cred_path} not found, using defaults")
        _credentials = {"url": "http://localhost:8529", "user": "root", "password": "password"}

    if args.mysql_credentials:
        mysql_path = Path(args.mysql_credentials)
        if mysql_path.exists():
            with open(mysql_path) as f:
                _mysql_credentials = yaml.safe_load(f)
            print(f"Loaded MySQL credentials from {mysql_path}")
        else:
            print(f"Warning: MySQL credentials file {mysql_path} not found")

    print(f"Starting QA Browser at http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()