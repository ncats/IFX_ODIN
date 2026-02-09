import argparse
import json
from pathlib import Path
from typing import Optional

import urllib3
import yaml
from arango import ArangoClient
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import uvicorn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="ArangoDB QA Browser")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# -- Global state set at startup --
_client: Optional[ArangoClient] = None
_credentials: dict = {}


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
    sys_db = get_sys_db()
    databases = [db for db in sys_db.databases() if not db.startswith("_")]
    return templates.TemplateResponse("home.html", {
        "request": request,
        "databases": databases,
        "arango_url": _credentials.get("url", "http://localhost:8529"),
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
    parser = argparse.ArgumentParser(description="ArangoDB QA Browser")
    parser.add_argument("--credentials", "-c",
                        default="./src/use_cases/secrets/local_arangodb.yaml",
                        help="Path to ArangoDB credentials YAML file")
    parser.add_argument("--port", "-p", type=int, default=8050)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    global _credentials
    cred_path = Path(args.credentials)
    if cred_path.exists():
        with open(cred_path) as f:
            _credentials = yaml.safe_load(f)
        print(f"Loaded credentials from {cred_path}")
    else:
        print(f"Warning: {cred_path} not found, using defaults")
        _credentials = {"url": "http://localhost:8529", "user": "root", "password": "password"}

    print(f"Starting QA Browser at http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()