"""Variant resolver — local graph + live MyVariant.info enrichment.

Combines the harmonized variant graph (instant local lookup) with optional
live enrichment from MyVariant.info for:
  - dbSNP identifiers and gene annotations
  - ClinVar clinical significance, conditions, review status
  - Population frequencies (gnomAD exome/genome)
  - Deleteriousness scores (CADD, SIFT, PolyPhen-2, REVEL, MetaLR)
  - COSMIC somatic annotations

Designed for the QA Browser variant app "Resolver" tab.  Each function can
be called independently; the top-level ``resolve_and_enrich`` orchestrates
a full resolve+enrich pass for a list of user queries.
"""
from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from src.qa_browser.variant_id_graph import (
        VariantGraphData, search_variants, _variant_lookup_aliases,
    )
except ImportError:
    from variant_id_graph import (
        VariantGraphData, search_variants, _variant_lookup_aliases,
    )

# ── API endpoints ────────────────────────────────────────────────────────

MYVARIANT_API = "https://myvariant.info/v1"
_INTERNAL_ID_RE = re.compile(r"^IFX[A-Za-z]+:")  # skip IFX IDs in API calls

logger = logging.getLogger(__name__)

# ── MyVariant field catalog ──────────────────────────────────────────────

MYVARIANT_FIELD_CATALOG: list[dict[str, Any]] = [
    # ── Identifiers ────────────────────────────────────────────────────
    {"key": "dbsnp.rsid",                      "label": "dbSNP rsID",             "category": "Identifiers",           "default": True},
    {"key": "dbsnp.gene.symbol",               "label": "dbSNP Gene Symbol",      "category": "Identifiers",           "default": True},
    {"key": "clinvar.variant_id",              "label": "ClinVar Variant ID",     "category": "Identifiers",           "default": True},
    {"key": "clinvar.allele_id",               "label": "ClinVar Allele ID",      "category": "Identifiers",           "default": True},
    {"key": "clinvar.rcv.accession",           "label": "ClinVar RCV",            "category": "Identifiers",           "default": True},
    {"key": "clinvar.hgvs.coding",             "label": "HGVS Coding",            "category": "Identifiers",           "default": True},
    {"key": "clinvar.hgvs.genomic",            "label": "HGVS Genomic",           "category": "Identifiers",           "default": True},
    # ── ClinVar Clinical ───────────────────────────────────────────────
    {"key": "clinvar.rcv.clinical_significance", "label": "Clinical Significance", "category": "ClinVar Clinical",     "default": True},
    {"key": "clinvar.rcv.conditions.name",     "label": "Condition Name",          "category": "ClinVar Clinical",     "default": True},
    {"key": "clinvar.rcv.review_status",       "label": "Review Status",           "category": "ClinVar Clinical",     "default": True},
    # ── Population Frequencies ─────────────────────────────────────────
    {"key": "gnomad_exome.af.af",              "label": "gnomAD Exome AF",         "category": "Population Frequencies", "default": True},
    {"key": "gnomad_exome.af.af_afr",          "label": "gnomAD AFR",              "category": "Population Frequencies", "default": True},
    {"key": "gnomad_exome.af.af_eas",          "label": "gnomAD EAS",              "category": "Population Frequencies", "default": True},
    {"key": "gnomad_exome.af.af_nfe",          "label": "gnomAD NFE",              "category": "Population Frequencies", "default": True},
    {"key": "gnomad_genome.af.af",             "label": "gnomAD Genome AF",        "category": "Population Frequencies", "default": True},
    # ── Deleteriousness ────────────────────────────────────────────────
    {"key": "cadd.phred",                      "label": "CADD Phred",              "category": "Deleteriousness",       "default": True},
    {"key": "dbnsfp.sift.score",               "label": "SIFT Score",              "category": "Deleteriousness",       "default": True},
    {"key": "dbnsfp.polyphen2.hdiv.score",     "label": "PolyPhen-2 HDIV",         "category": "Deleteriousness",       "default": True},
    {"key": "dbnsfp.revel.score",              "label": "REVEL Score",              "category": "Deleteriousness",       "default": True},
    {"key": "dbnsfp.metalr.score",             "label": "MetaLR",                   "category": "Deleteriousness",       "default": False},
    # ── COSMIC ─────────────────────────────────────────────────────────
    {"key": "cosmic.cosmic_id",                "label": "COSMIC ID",                "category": "COSMIC",               "default": False},
    {"key": "cosmic.tumor_site",               "label": "Tumor Site",               "category": "COSMIC",               "default": False},
]

MYVARIANT_DEFAULT_FIELDS: list[str] = [f["key"] for f in MYVARIANT_FIELD_CATALOG if f["default"]]
_MYVARIANT_VALID_KEYS: set[str] = {f["key"] for f in MYVARIANT_FIELD_CATALOG}

# Scopes by query type
_SCOPES_BY_TYPE: dict[str, str] = {
    "rsid": "dbsnp.rsid",
    "clinvar_id": "clinvar.variant_id",
    "hgvs": "_id",
    "gene_symbol": "dbsnp.gene.symbol",
    "cosmic": "cosmic.cosmic_id",
    "freetext": "dbsnp.rsid,clinvar.variant_id,_id,cosmic.cosmic_id",
}


def get_myvariant_field_catalog() -> list[dict[str, Any]]:
    """Return the full MyVariant field catalog for the frontend."""
    return MYVARIANT_FIELD_CATALOG


# ── HTTP session ─────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    s.trust_env = False
    s.verify = False
    retry = Retry(
        total=3, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"), raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    return s


# ── Query type detection ─────────────────────────────────────────────────

def _detect_query_type(query: str) -> tuple[str, str]:
    """Classify a user query and return (query_type, cleaned_value)."""
    q = query.strip()
    if not q:
        return ("freetext", q)

    # Strip common namespace prefixes the user may copy from the app
    m_dbsnp = re.fullmatch(r"(?:dbSNP:)?(rs\d+)", q, re.I)
    if m_dbsnp:
        return ("rsid", m_dbsnp.group(1).lower())

    # ClinVar variation ID: "ClinVarVariation:12345", "ClinVar:12345", or "ClinVarAllele:12345"
    m_cv = re.fullmatch(r"(?:ClinVar(?:Variation|Allele)?:)(\d+)", q, re.I)
    if m_cv:
        return ("clinvar_id", m_cv.group(1))

    # Bare integer — only treat as ClinVar ID if >= 4 digits
    # (low integers like 12 or 99 are too ambiguous)
    m_bare = re.fullmatch(r"(\d+)", q)
    if m_bare and len(m_bare.group(1)) >= 4:
        return ("clinvar_id", m_bare.group(1))

    # HGVS notation: contains :c. :p. :g. :n. :m. :r. :o.
    if re.search(r":\s*[cpgnmro]\.", q):
        return ("hgvs", q)

    # Bare protein HGVS: p.Val600Glu or p.V600E (no transcript prefix)
    if re.match(r"p\.[A-Z]", q, re.I):
        return ("hgvs", q)

    # Chromosomal HGVS: chr1:g.12345A>G (including chrX, chrY, chrM)
    if re.match(r"chr[\dXYM]+:\s*g\.\d+", q, re.I):
        return ("hgvs", q)

    # COSMIC ID: COSM/COSV followed by digits
    if re.fullmatch(r"COS[MV]\d+", q, re.I):
        return ("cosmic", q.upper())

    # Gene symbol: starts with uppercase letter, 2-10 alphanumeric chars
    # Accept mixed case (e.g. "Braf") and uppercase it for consistency
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9]{1,9}", q) and not q.isdigit():
        return ("gene_symbol", q.upper())

    return ("freetext", q)


# ── MyVariant.info enrichment ────────────────────────────────────────────

def _dig(obj: Any, dotpath: str) -> Any:
    """Navigate nested dicts/lists by dot-separated key path.

    When a list of dicts is encountered mid-path, collect the value from
    *every* element (not just the first) so that multi-RCV ClinVar records,
    multi-transcript scores, etc. are fully preserved.
    """
    parts = dotpath.split(".")
    current = obj
    for i, part in enumerate(parts):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and current:
            # Collect from all list elements, then continue digging
            remaining = ".".join(parts[i:])
            collected = []
            for item in current:
                val = _dig(item, remaining) if isinstance(item, dict) else None
                if val is None:
                    continue
                if isinstance(val, list):
                    collected.extend(val)
                else:
                    collected.append(val)
            return collected if collected else None
        else:
            return None
        if current is None:
            return None
    return current


def _format_value(val: Any) -> str | None:
    """Format a MyVariant field value for display."""
    if val is None:
        return None
    if isinstance(val, list):
        flat = []
        for item in val:
            if isinstance(item, dict):
                flat.append(str(next(iter(item.values()), "")))
            else:
                flat.append(str(item))
        joined = "|".join(s for s in flat if s and s.lower() not in ("none", "nan"))
        return joined or None
    s = str(val).strip()
    return None if s.lower() in ("", "none", "nan") else s


def _parse_myvariant_hit(hit: dict, catalog: list[dict[str, Any]]) -> dict[str, str | None]:
    """Extract structured fields from a raw MyVariant.info hit."""
    result: dict[str, str | None] = {}
    if not hit:
        return result
    # Always include the MyVariant native _id
    result["_id"] = hit.get("_id")
    result["_score"] = str(hit.get("_score", "")) if hit.get("_score") else None

    for entry in catalog:
        key = entry["key"]
        val = _dig(hit, key)
        result[key] = _format_value(val)
    return result


def enrich_myvariant(
    query: str,
    query_type: str,
    fields: list[str],
    session: requests.Session | None = None,
    timeout: int = 15,
) -> dict[str, Any] | None:
    """Query MyVariant.info for variant annotations.

    Returns parsed hit dict for single-variant queries, or a list of parsed
    hits for gene_symbol queries (which can match many variants).
    """
    if not query:
        return None
    session = session or _build_session()

    scopes = _SCOPES_BY_TYPE.get(query_type, _SCOPES_BY_TYPE["freetext"])
    size = 10 if query_type == "gene_symbol" else 5

    # Build fields param — always request the base needed fields
    requested_fields = list(fields) if fields else list(MYVARIANT_DEFAULT_FIELDS)
    requested_fields = [f for f in requested_fields if f in _MYVARIANT_VALID_KEYS]
    if not requested_fields:
        requested_fields = list(MYVARIANT_DEFAULT_FIELDS)

    # Catalog subset for parsing
    catalog = [e for e in MYVARIANT_FIELD_CATALOG if e["key"] in requested_fields]

    try:
        resp = session.post(
            f"{MYVARIANT_API}/query",
            data={
                "q": query,
                "scopes": scopes,
                "fields": ",".join(requested_fields),
                "size": size,
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            logger.debug("MyVariant query failed for %s: HTTP %d", query, resp.status_code)
            return None

        data = resp.json()

        # MyVariant can return a dict (single query) or list (batch)
        hits: list[dict] = []
        if isinstance(data, dict):
            hits = data.get("hits", [])
        elif isinstance(data, list):
            # batch response: each item has hits
            for item in data:
                if isinstance(item, dict) and not item.get("notfound"):
                    hits.append(item)

        if not hits:
            return None

        if query_type == "gene_symbol":
            # Return all parsed hits for gene queries
            parsed = [_parse_myvariant_hit(h, catalog) for h in hits if not h.get("notfound")]
            return {"hits": parsed, "total": len(parsed)} if parsed else None
        else:
            # Return first non-notfound hit
            for h in hits:
                if not h.get("notfound"):
                    return _parse_myvariant_hit(h, catalog)
            return None

    except Exception as exc:
        logger.debug("MyVariant query failed for %s: %s", query, exc)
        return None


# ── Local graph lookup ───────────────────────────────────────────────────

def _local_lookup(
    query: str,
    data: VariantGraphData,
    cleaned: str | None = None,
) -> list[dict[str, str]]:
    """Search the local variant graph for matching nodes.

    Uses the exact-match alias index first (``ids_to_variants``), then
    falls back to the substring search.  Exact matches always appear
    before substring-only matches so that e.g. ``rs334`` returns the
    HBB sickle-cell variant ahead of ``rs334704``.

    When a *cleaned* value is provided (e.g. ``"12583"`` extracted from
    ``"ClinVar:12583"``), the alias index is also searched with the
    cleaned form so that prefix-stripped queries still resolve locally.
    """
    # 1. Exact-match via indexed aliases
    exact_ids: list[str] = []
    for alias in _variant_lookup_aliases(query):
        exact_ids.extend(data.ids_to_variants.get(alias, []))
    # Also try the cleaned value if it differs from the raw query
    if cleaned and cleaned != query:
        for alias in _variant_lookup_aliases(cleaned):
            exact_ids.extend(data.ids_to_variants.get(alias, []))
    # Deduplicate while preserving order
    seen: set[str] = set()
    exact_nodes: list[dict[str, str]] = []
    for vid in exact_ids:
        if vid not in seen:
            seen.add(vid)
            node = data.nodes_by_id.get(vid)
            if node:
                exact_nodes.append(node)

    # 2. Substring search as fallback (try both raw and cleaned queries)
    for search_q in dict.fromkeys([query, cleaned] if cleaned else [query]):
        payload = search_variants(data, q=search_q, per_page=10)
        for row in payload.get("rows", []):
            vid = row.get("variant_id", "")
            if vid not in seen:
                seen.add(vid)
                exact_nodes.append(row)

    return exact_nodes[:10]


# ── Per-query orchestrator ───────────────────────────────────────────────

def _enrich_one(
    query: str,
    data: VariantGraphData,
    enable_myvariant: bool,
    myvariant_fields: list[str],
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Resolve a single query: local lookup + optional MyVariant enrichment."""
    session = session or _build_session()
    query = query.strip()
    if not query:
        return {
            "query": query,
            "query_type": "empty",
            "local_hits": [],
            "myvariant": None,
            "status": "empty",
        }

    query_type, cleaned = _detect_query_type(query)

    # Local graph lookup — pass cleaned value so prefix-stripped queries
    # (e.g. "ClinVar:12583" → "12583") also resolve via the alias index
    local_hits = _local_lookup(query, data, cleaned=cleaned)

    # MyVariant.info enrichment
    myvariant_result = None
    if enable_myvariant and not _INTERNAL_ID_RE.match(query):
        myvariant_result = enrich_myvariant(
            cleaned, query_type, myvariant_fields, session=session,
        )

    # Status determination
    has_local = len(local_hits) > 0
    has_api = myvariant_result is not None
    if has_local and has_api:
        status = "both"
    elif has_local:
        status = "local_only"
    elif has_api:
        status = "api_only"
    else:
        status = "not_found"

    return {
        "query": query,
        "query_type": query_type,
        "local_hits": local_hits,
        "myvariant": myvariant_result,
        "status": status,
    }


# ── Batch orchestrator ───────────────────────────────────────────────────

def resolve_and_enrich(
    queries: list[str],
    data: VariantGraphData,
    enable_myvariant: bool = True,
    myvariant_fields: list[str] | None = None,
    workers: int = 4,
) -> dict[str, Any]:
    """Full resolve+enrich for a batch of user queries.

    Returns a dict with ``results`` (list, one per query) and ``stats``.
    """
    fields = list(myvariant_fields) if myvariant_fields else list(MYVARIANT_DEFAULT_FIELDS)
    fields = [f for f in fields if f in _MYVARIANT_VALID_KEYS]
    if not fields:
        fields = list(MYVARIANT_DEFAULT_FIELDS)

    results: list[dict[str, Any]] = [{} for _ in range(len(queries))]
    stats_lock = Lock()
    stats = {
        "total": len(queries),
        "local_resolved": 0,
        "myvariant_found": 0,
        "enrichment_enabled": enable_myvariant,
    }

    def worker(idx: int, query: str) -> tuple[int, dict]:
        session = _build_session()
        result = _enrich_one(
            query, data,
            enable_myvariant=enable_myvariant,
            myvariant_fields=fields,
            session=session,
        )
        with stats_lock:
            if result.get("local_hits"):
                stats["local_resolved"] += 1
            if result.get("myvariant"):
                stats["myvariant_found"] += 1
        return idx, result

    actual_workers = min(workers, len(queries)) or 1
    with ThreadPoolExecutor(max_workers=actual_workers) as pool:
        futures = {
            pool.submit(worker, i, q): i
            for i, q in enumerate(queries)
        }
        for future in as_completed(futures):
            try:
                idx, result = future.result()
                results[idx] = result
            except Exception as exc:
                idx = futures[future]
                results[idx] = {
                    "query": queries[idx],
                    "query_type": "error",
                    "local_hits": [],
                    "myvariant": None,
                    "status": "error",
                    "error": str(exc),
                }

    return {"results": results, "stats": stats}
