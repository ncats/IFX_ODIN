"""Enhanced drug resolver — local graph + live API enrichment.

Combines the 412K harmonized drug graph (instant local lookup) with optional
live enrichment from:
  1. NCATS Resolver — structure properties, SMILES, UNII, CAS, dev phase
  2. Pharos GraphQL — drug-target activities, MOA, TDL classes
  3. Inxight/GSRS  — regulatory status, substance class, therapeutic class
  4. openFDA       — drug labels, adverse reactions, boxed warnings
  5. ChEBI OLS     — chemical class hierarchy

Designed for the QA Browser drug app "Resolver" tab.  Each function can be
called independently; the top-level ``resolve_and_enrich`` orchestrates a
full resolve+enrich pass for a list of user queries.
"""
from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from threading import Lock
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from src.qa_browser.drug_id_graph import DrugGraphData, search_drugs
except ImportError:
    from drug_id_graph import DrugGraphData, search_drugs

# ── API endpoints ────────────────────────────────────────────────────────

NCATS_RESOLVER_BASE = "https://resolver.ncats.nih.gov/resolver"
PHAROS_API = "https://pharos-api.ncats.io/graphql"
INXIGHT_API = "https://drugs.ncats.io/api/v1/substances"
CHEBI_OLS_API = "https://www.ebi.ac.uk/ols4/api"
OPENFDA_LABEL_API = "https://api.fda.gov/drug/label.json"

NCATS_PROPS = [
    "smiles", "tpsa", "logp", "logd", "hbd", "hba", "drug", "cns",
    "description", "cid", "unii", "chembl", "chebi", "cas", "names",
    "devphase", "molWeight", "molForm",
]

CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")

logger = logging.getLogger(__name__)

# ── HTTP session ─────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    s.trust_env = False
    s.verify = False
    retry = Retry(
        total=3, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"), raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    return s


def _cv(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in ("", "nan", "none") else s


def _shorten(val: Any, limit: int = 500) -> str | None:
    if not val:
        return None
    text = re.sub(r"\s+", " ", str(val)).strip()
    return text[:limit - 3].rstrip() + "..." if len(text) > limit else text


def _clean_html(val: Any) -> str | None:
    if not val:
        return None
    text = re.sub(r"<[^>]+>", " ", str(val))
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text or None


def _first_year(val: Any) -> str | None:
    if not val:
        return None
    m = re.search(r"\b((?:19|20)\d{2})\b", str(val))
    return m.group(1) if m else None


def _uniq_join(values: list, limit: int | None = None) -> str:
    seen: list[str] = []
    for v in values:
        s = str(v).strip() if v is not None else ""
        if s and s not in seen:
            seen.append(s)
    if limit is not None:
        seen = seen[:limit]
    return "|".join(seen)


# ═════════════════════════════════════════════════════════════════════════
# STEP 1: LOCAL GRAPH LOOKUP
# ═════════════════════════════════════════════════════════════════════════

def resolve_local(data: DrugGraphData, queries: list[str]) -> list[dict[str, Any]]:
    """Resolve queries against the local harmonized drug graph.

    Returns one result dict per query with ``local_hits`` (list of matching
    drug node dicts) and ``query``.
    """
    results: list[dict[str, Any]] = []
    for q in queries:
        q = q.strip()
        if not q:
            results.append({"query": q, "local_hits": [], "resolved": False})
            continue
        payload = search_drugs(data, q=q, per_page=10)
        hits = payload.get("rows", [])
        results.append({
            "query": q,
            "local_hits": hits,
            "resolved": len(hits) > 0,
            "total_local_matches": payload.get("total", 0),
        })
    return results


# ═════════════════════════════════════════════════════════════════════════
# STEP 2a: NCATS RESOLVER
# ═════════════════════════════════════════════════════════════════════════

def enrich_ncats_resolver(
    query: str,
    session: requests.Session | None = None,
    api_key: str = "5fd5bb2a05eb6195",
    timeout: int = 30,
) -> dict[str, Any] | None:
    """Query the NCATS chemical resolver for a single identifier."""
    session = session or _build_session()
    try:
        url = f"{NCATS_RESOLVER_BASE}/{'/'.join(NCATS_PROPS)}/"
        params = {
            "structure": query,
            "standardize": "CHARGE_NORMALIZE",
            "force": "false",
            "apikey": api_key,
            "useApproxMatch": "false",
            "useContains": "false",
        }
        r = session.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            return None
        text = r.text.strip()
        if not text:
            return None
        values = text.split("\n")[0].split("\t")
        if len(values) < 2:
            return None
        raw: dict[str, str] = {}
        for i, prop in enumerate(NCATS_PROPS):
            idx = i + 1
            if idx < len(values):
                v = values[idx].strip()
                if v and v != "0":
                    v = re.sub(r"^\[NO STEREO\]\s*", "", v)
                    raw[prop] = v
        if not raw:
            return None
        names_raw = raw.get("names", "")
        name_list = [n.strip() for n in names_raw.split("|") if n.strip()]
        return {
            "ncats_resolved_name": name_list[0] if name_list else None,
            "ncats_smiles": raw.get("smiles"),
            "ncats_formula": raw.get("molForm"),
            "ncats_mol_weight": raw.get("molWeight"),
            "ncats_cid": raw.get("cid"),
            "ncats_unii": raw.get("unii"),
            "ncats_chembl": raw.get("chembl"),
            "ncats_chebi": raw.get("chebi"),
            "ncats_cas": raw.get("cas"),
            "ncats_tpsa": raw.get("tpsa"),
            "ncats_logp": raw.get("logp"),
            "ncats_logd": raw.get("logd"),
            "ncats_hbd": raw.get("hbd"),
            "ncats_hba": raw.get("hba"),
            "ncats_drug_flag": raw.get("drug"),
            "ncats_cns_flag": raw.get("cns"),
            "ncats_dev_phase": raw.get("devphase"),
            "ncats_description": _shorten(raw.get("description"), 300),
            "ncats_all_names": "|".join(name_list[:15]),
        }
    except Exception as exc:
        logger.debug("NCATS resolver failed for %s: %s", query, exc)
        return None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2b: PHAROS GRAPHQL
# ═════════════════════════════════════════════════════════════════════════

_PHAROS_QUERY = """
query ligandDetails($ligid: String!) {
  ligand(ligid: $ligid) {
    ligid
    name
    description
    isdrug
    smiles
    activities {
      type
      value
      moa
      target {
        sym
        name
        tdl
        fam
      }
    }
  }
}
"""


def _clean_name_for_pharos(name: str | None) -> str | None:
    if not name:
        return None
    n = str(name).strip()
    for suffix in [" HYDROCHLORIDE", " HCL", " SODIUM", " POTASSIUM",
                   " MALEATE", " MESYLATE", " TARTRATE", " FUMARATE",
                   " SULFATE", " ACETATE", " CITRATE", " BESYLATE",
                   " SUCCINATE", " TOSYLATE", " BROMIDE", " CHLORIDE"]:
        if n.upper().endswith(suffix):
            n = n[:len(n) - len(suffix)].strip()
    n = re.sub(r"^[\(\[]?[RSrs\u00b1\+\-]+[\)\]]?-?\s*", "", n)
    n = re.sub(r"^trans-|^cis-", "", n, flags=re.IGNORECASE)
    n = re.sub(r",\s*[\(\[].+$", "", n)
    return n.strip() or None


def enrich_pharos(
    name: str,
    session: requests.Session | None = None,
    timeout: int = 20,
) -> dict[str, Any] | None:
    """Query Pharos GraphQL for drug-target activities."""
    if not name:
        return None
    session = session or _build_session()

    # Try cleaned name first, then raw
    for lookup in [_clean_name_for_pharos(name), name]:
        if not lookup:
            continue
        try:
            r = session.post(
                PHAROS_API,
                json={"query": _PHAROS_QUERY, "variables": {"ligid": lookup}},
                timeout=timeout,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            if data.get("errors"):
                continue
            lig = (data.get("data") or {}).get("ligand")
            if not lig:
                continue

            activities = lig.get("activities") or []
            seen: dict[str, dict] = {}
            for act in activities:
                tgt = act.get("target") or {}
                sym = tgt.get("sym")
                if not sym:
                    continue
                entry = {
                    "symbol": sym,
                    "name": tgt.get("name"),
                    "tdl": tgt.get("tdl"),
                    "family": tgt.get("fam"),
                    "activity_type": act.get("type"),
                    "activity_value": act.get("value"),
                    "moa": act.get("moa"),
                }
                if sym not in seen:
                    seen[sym] = entry
                elif entry.get("moa") and not seen[sym].get("moa"):
                    seen[sym] = entry
            targets = list(seen.values())
            moa_targets = [t for t in targets if t.get("moa")]

            return {
                "pharos_name": lig.get("name"),
                "pharos_isdrug": str(lig.get("isdrug", "")),
                "pharos_description": _shorten(lig.get("description"), 300),
                "pharos_n_targets": len(targets),
                "pharos_n_activities": len(activities),
                "pharos_targets": _uniq_join([t["symbol"] for t in targets], 20),
                "pharos_target_tdls": _uniq_join(sorted(set(t["tdl"] for t in targets if t.get("tdl")))),
                "pharos_target_families": _uniq_join(sorted(set(t["family"] for t in targets if t.get("family")))),
                "pharos_moa_targets": _uniq_join([t["symbol"] for t in moa_targets], 15),
                "pharos_moa_values": _uniq_join(sorted(set(str(t["moa"]) for t in moa_targets if t.get("moa"))), 10),
                "pharos_target_details": targets[:50],
            }
        except Exception as exc:
            logger.debug("Pharos failed for %s: %s", lookup, exc)
    return None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2c: INXIGHT / GSRS
# ═════════════════════════════════════════════════════════════════════════

def _parse_inxight_drug_page(html: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    m = re.search(
        r'<span[^>]*id=["\']label-status["\'][^>]*title=["\']([^"\']+)["\'][^>]*>(.*?)</span>',
        html, flags=re.I | re.DOTALL,
    )
    if m:
        parsed["inxight_development_status"] = _clean_html(m.group(1))
        parsed["inxight_development_status_year"] = _first_year(m.group(2))
    first_m = re.search(r"First approved in(?:&nbsp;|&#160;|\s)+((?:19|20)\d{2})", html, re.I)
    if first_m:
        parsed["inxight_first_approval_year"] = first_m.group(1)
    return {k: v for k, v in parsed.items() if _cv(v)}


def enrich_inxight(
    unii: str,
    session: requests.Session | None = None,
    timeout: int = 20,
) -> dict[str, Any] | None:
    """Query Inxight/GSRS by UNII for regulatory + target context."""
    if not unii:
        return None
    session = session or _build_session()
    unii = re.sub(r"^\[NO STEREO\]\s*", "", str(unii).split("|")[0].strip()).strip()
    if not unii:
        return None

    result: dict[str, Any] = {}

    # Drug page scrape for development badge
    try:
        page_resp = session.get(f"https://drugs.ncats.io/drug/{unii}", timeout=timeout)
        if page_resp.status_code == 200:
            page_data = _parse_inxight_drug_page(page_resp.text)
            if page_data:
                page_data["inxight_drug_url"] = f"https://drugs.ncats.io/drug/{unii}"
                result.update(page_data)
    except Exception:
        pass

    # API search
    try:
        r = session.get(f"{INXIGHT_API}/search", params={"q": unii}, timeout=timeout)
        if r.status_code != 200:
            return result or None
        hits = (r.json().get("content") or [])
        if not hits:
            return result or None
        d = next((h for h in hits if str(h.get("approvalID", "")).upper() == unii.upper()), hits[0])
        uuid = d.get("uuid")
        if not uuid:
            return result or None

        # Detail
        detail_resp = session.get(f"{INXIGHT_API}({uuid})", timeout=timeout)
        if detail_resp.status_code == 200:
            d = detail_resp.json()

        result["inxight_name"] = d.get("_name")
        result["inxight_unii"] = d.get("approvalID")
        result["inxight_substance_class"] = d.get("substanceClass")
        result["inxight_status"] = d.get("status")

        # Codes → therapeutic class
        codes_resp = session.get(f"{INXIGHT_API}({uuid})/codes", params={"top": 100}, timeout=timeout)
        if codes_resp.status_code == 200:
            codes = codes_resp.json() or []
            result["inxight_therapeutic_class"] = _uniq_join(
                [c.get("comments") for c in codes
                 if c.get("codeSystem") in ("WHO-ATC", "WHO-VATC") and c.get("comments")],
                limit=8,
            )

        # Relationships → targets
        rels_resp = session.get(f"{INXIGHT_API}({uuid})/relationships", params={"top": 100}, timeout=timeout)
        if rels_resp.status_code == 200:
            rels = rels_resp.json() or []
            target_names: list[str] = []
            moa_values: list[str] = []
            for rel in rels:
                rtype = rel.get("type", "")
                related = (rel.get("relatedSubstance") or {}).get("refPname", "")
                if rtype.upper().startswith("TARGET->") or rtype.upper().startswith("TARGET ORGANISM->"):
                    if related:
                        target_names.append(related)
                    moa = rtype.split("->", 1)[1].strip() if "->" in rtype else None
                    if moa:
                        moa_values.append(moa)
            result["inxight_targets"] = _uniq_join(target_names, 15)
            result["inxight_moa_values"] = _uniq_join(moa_values, 10)
            result["inxight_n_relationships"] = len(rels)

        return result
    except Exception as exc:
        logger.debug("Inxight failed for %s: %s", unii, exc)
        return result or None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2d: openFDA
# ═════════════════════════════════════════════════════════════════════════

def enrich_openfda(
    name_candidates: list[str],
    session: requests.Session | None = None,
    timeout: int = 15,
) -> dict[str, Any] | None:
    """Search openFDA drug labels for adverse reactions and warnings."""
    candidates = list(dict.fromkeys(c.strip() for c in name_candidates if c and c.strip()))[:8]
    if not candidates:
        return None
    session = session or _build_session()
    fields = [
        "openfda.generic_name.exact",
        "openfda.substance_name.exact",
        "openfda.brand_name.exact",
        "active_ingredient",
    ]
    for candidate in candidates:
        safe = candidate.replace('"', '\\"')
        for field in fields:
            try:
                resp = session.get(
                    OPENFDA_LABEL_API,
                    params={"search": f'{field}:"{safe}"', "limit": 1},
                    timeout=timeout,
                )
                if resp.status_code != 200:
                    continue
                results = (resp.json().get("results") or [])
                if not results:
                    continue
                label = results[0]
                openfda = label.get("openfda") or {}
                return {
                    "openfda_generic_name": _uniq_join(openfda.get("generic_name") or [], 3),
                    "openfda_brand_name": _uniq_join(openfda.get("brand_name") or [], 3),
                    "openfda_product_type": _uniq_join(openfda.get("product_type") or [], 3),
                    "openfda_route": _uniq_join(openfda.get("route") or [], 5),
                    "openfda_manufacturer": _uniq_join(openfda.get("manufacturer_name") or [], 3),
                    "openfda_adverse_reactions": _shorten(" ".join(label.get("adverse_reactions") or []), 800),
                    "openfda_boxed_warning": _shorten(" ".join(label.get("boxed_warning") or []), 500),
                    "openfda_warnings": _shorten(" ".join(label.get("warnings") or []), 500),
                }
            except Exception:
                continue
    return None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2e: ChEBI OLS
# ═════════════════════════════════════════════════════════════════════════

def enrich_chebi(
    chebi_id: str,
    session: requests.Session | None = None,
    timeout: int = 15,
) -> dict[str, Any] | None:
    """Look up ChEBI chemical class hierarchy via OLS4."""
    if not chebi_id:
        return None
    m = re.search(r"CHEBI:\d+", str(chebi_id).upper())
    if not m:
        return None
    session = session or _build_session()
    norm_id = m.group(0)
    iri = f"http://purl.obolibrary.org/obo/{norm_id.replace(':', '_')}"
    encoded = requests.utils.quote(requests.utils.quote(iri, safe=""), safe="")
    try:
        term_resp = session.get(f"{CHEBI_OLS_API}/ontologies/chebi/terms/{encoded}", timeout=timeout)
        if term_resp.status_code != 200:
            return None
        term = term_resp.json()
        parent_href = ((term.get("_links") or {}).get("hierarchicalParents") or {}).get("href")
        parents: list[str] = []
        if parent_href:
            p_resp = session.get(parent_href, timeout=timeout)
            if p_resp.status_code == 200:
                terms = ((p_resp.json().get("_embedded") or {}).get("terms")) or []
                parents = [t.get("label") for t in terms if t.get("label")]
        generic = {"entity", "chemical entity", "molecular entity", "group", "application", "role", "subatomic particle"}
        informative = [p for p in parents if p.lower() not in generic]
        return {
            "chebi_label": term.get("label"),
            "chebi_definition": _shorten(" ".join(term.get("description") or []), 300),
            "chebi_chemical_class": "|".join(informative[:5]) if informative else None,
        }
    except Exception as exc:
        logger.debug("ChEBI failed for %s: %s", chebi_id, exc)
        return None


# ═════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════

def _enrich_one(
    local_result: dict[str, Any],
    enable_ncats: bool,
    enable_pharos: bool,
    enable_inxight: bool,
    enable_openfda: bool,
    enable_chebi: bool,
    delay: float,
) -> dict[str, Any]:
    """Enrich a single local-resolved result with live API data."""
    session = _build_session()
    enrichment: dict[str, Any] = {}
    query = local_result["query"]
    hits = local_result.get("local_hits") or []
    best = hits[0] if hits else {}

    # Determine lookup keys from local hit or raw query
    name = best.get("standard_name") or query
    unii = best.get("unii") or ""
    chebi_id = best.get("chebi_id") or ""
    chembl_id = best.get("chembl_id") or ""

    # NCATS Resolver
    if enable_ncats:
        # Try UNII, then ChEMBL, then name, then raw query
        for lookup in [unii, chembl_id, name, query]:
            if not lookup:
                continue
            ncats = enrich_ncats_resolver(lookup, session=session)
            if ncats:
                enrichment["ncats_resolver"] = ncats
                # Fill in any missing IDs from NCATS
                if not unii and ncats.get("ncats_unii"):
                    unii = ncats["ncats_unii"]
                if not chebi_id and ncats.get("ncats_chebi"):
                    chebi_id = ncats["ncats_chebi"]
                if not chembl_id and ncats.get("ncats_chembl"):
                    chembl_id = ncats["ncats_chembl"]
                # Update name so downstream lookups (Pharos, openFDA) use
                # the resolved chemical name instead of the raw query ID
                resolved_name = ncats.get("ncats_resolved_name")
                if resolved_name and (not name or name == query):
                    name = resolved_name
                break
            time.sleep(delay)

    # Pharos — try resolved name, then ChEMBL ID as fallback
    if enable_pharos:
        pharos = enrich_pharos(name, session=session)
        if not pharos and chembl_id:
            pharos = enrich_pharos(chembl_id, session=session)
        if pharos:
            enrichment["pharos"] = pharos
        time.sleep(delay)

    # Inxight
    if enable_inxight and unii:
        inxight = enrich_inxight(unii, session=session)
        if inxight:
            enrichment["inxight"] = inxight
            # Collect name candidates for openFDA
            if not name or name == query:
                name = inxight.get("inxight_name") or name
        time.sleep(delay)

    # ChEBI
    if enable_chebi and chebi_id:
        chebi = enrich_chebi(chebi_id, session=session)
        if chebi:
            enrichment["chebi"] = chebi
        time.sleep(delay)

    # openFDA
    if enable_openfda:
        name_candidates = list(dict.fromkeys(filter(None, [
            name,
            _clean_name_for_pharos(name),
            (enrichment.get("inxight") or {}).get("inxight_name"),
            (enrichment.get("pharos") or {}).get("pharos_name"),
            (enrichment.get("ncats_resolver") or {}).get("ncats_resolved_name"),
        ])))
        if name_candidates:
            openfda = enrich_openfda(name_candidates, session=session)
            if openfda:
                enrichment["openfda"] = openfda

    enrichment["sources_queried"] = [
        s for s, enabled in [
            ("ncats_resolver", enable_ncats),
            ("pharos", enable_pharos),
            ("inxight", enable_inxight),
            ("chebi", enable_chebi),
            ("openfda", enable_openfda),
        ] if enabled
    ]
    enrichment["sources_found"] = [
        s for s in ["ncats_resolver", "pharos", "inxight", "chebi", "openfda"]
        if s in enrichment
    ]

    return {**local_result, "enrichment": enrichment}


def resolve_and_enrich(
    data: DrugGraphData,
    queries: list[str],
    enable_ncats: bool = True,
    enable_pharos: bool = True,
    enable_inxight: bool = True,
    enable_openfda: bool = True,
    enable_chebi: bool = True,
    workers: int = 4,
    delay: float = 0.15,
) -> dict[str, Any]:
    """Full resolve+enrich for a batch of user queries.

    Returns a dict with ``results`` (list, one per query) and ``stats``.
    """
    # Step 1: local resolution (fast, no API)
    local_results = resolve_local(data, queries)

    # If no enrichment sources enabled, return local-only
    any_enrichment = any([enable_ncats, enable_pharos, enable_inxight, enable_openfda, enable_chebi])
    if not any_enrichment:
        return {
            "results": local_results,
            "stats": {
                "total": len(queries),
                "local_resolved": sum(1 for r in local_results if r.get("resolved")),
                "enrichment_enabled": False,
            },
        }

    # Step 2: threaded enrichment
    enriched: list[dict[str, Any]] = [{}] * len(local_results)
    stats_lock = Lock()
    stats = {
        "total": len(queries),
        "local_resolved": 0,
        "ncats_found": 0,
        "pharos_found": 0,
        "inxight_found": 0,
        "chebi_found": 0,
        "openfda_found": 0,
        "enrichment_enabled": True,
    }

    def worker(idx: int, local_result: dict) -> tuple[int, dict]:
        result = _enrich_one(
            local_result,
            enable_ncats=enable_ncats,
            enable_pharos=enable_pharos,
            enable_inxight=enable_inxight,
            enable_openfda=enable_openfda,
            enable_chebi=enable_chebi,
            delay=delay,
        )
        with stats_lock:
            if result.get("resolved"):
                stats["local_resolved"] += 1
            found = (result.get("enrichment") or {}).get("sources_found") or []
            for src in found:
                key = f"{src}_found"
                if key in stats:
                    stats[key] += 1
        return idx, result

    actual_workers = min(workers, len(local_results)) or 1
    with ThreadPoolExecutor(max_workers=actual_workers) as pool:
        futures = {
            pool.submit(worker, i, lr): i
            for i, lr in enumerate(local_results)
        }
        for future in as_completed(futures):
            try:
                idx, result = future.result()
                enriched[idx] = result
            except Exception as exc:
                idx = futures[future]
                enriched[idx] = {
                    **local_results[idx],
                    "enrichment": {"error": str(exc)},
                }

    return {"results": enriched, "stats": stats}
