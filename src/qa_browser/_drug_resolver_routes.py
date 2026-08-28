"""
Drug Resolver — FastAPI route definitions.

INTEGRATION INSTRUCTIONS
========================
After the other agent finishes editing drug_id_qa.html, merge these routes
into app.py:

1. Add import at the top of app.py:
       from drug_resolver import resolve_and_enrich

2. Paste the two route functions below into app.py alongside the existing
   /drug-id-qa/* routes (around line 6300+).

3. Make sure _load_drug_graph() is available (it already is in app.py).
"""
from fastapi import Query
from fastapi.responses import JSONResponse


# ── Paste these into app.py ──────────────────────────────────────────────


# @app.post("/drug-id-qa/api/resolve")
async def drug_id_qa_resolve(
    body: dict,
):
    """Batch resolve + enrich drug queries.

    Request body:
        {
            "queries": ["aspirin", "CHEMBL25", "UNII:R16CO5Y76E", ...],
            "enable_ncats": true,
            "enable_pharos": true,
            "enable_inxight": true,
            "enable_openfda": true,
            "enable_chebi": true,
            "workers": 4
        }
    """
    queries = body.get("queries", [])
    if not queries:
        return JSONResponse({"error": "No queries provided"}, status_code=400)
    if len(queries) > 50:
        return JSONResponse({"error": "Maximum 50 queries per request"}, status_code=400)

    data = _load_drug_graph()
    result = resolve_and_enrich(
        data,
        queries=queries,
        enable_ncats=body.get("enable_ncats", True),
        enable_pharos=body.get("enable_pharos", True),
        enable_inxight=body.get("enable_inxight", True),
        enable_openfda=body.get("enable_openfda", True),
        enable_chebi=body.get("enable_chebi", True),
        workers=min(int(body.get("workers", 4)), 8),
        delay=0.15,
    )
    return result


# @app.get("/drug-id-qa/api/resolve-quick")
async def drug_id_qa_resolve_quick(
    q: str = Query("", description="Pipe-separated queries"),
):
    """Quick local-only resolution (no enrichment, instant response).

    Example: /drug-id-qa/api/resolve-quick?q=aspirin|CHEMBL25|ibuprofen
    """
    if not q.strip():
        return {"results": [], "stats": {"total": 0}}
    queries = [s.strip() for s in q.split("|") if s.strip()][:50]
    data = _load_drug_graph()
    result = resolve_and_enrich(
        data,
        queries=queries,
        enable_ncats=False,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )
    return result
