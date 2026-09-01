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
from difflib import SequenceMatcher
from html import unescape
from threading import BoundedSemaphore, Lock
from typing import Any, Callable

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

# Full catalog of NCATS Resolver properties, organized by category.
# Each entry: (api_key, display_label, category, default_on)
NCATS_PROPERTY_CATALOG: list[dict[str, Any]] = [
    # ── Annotations ──────────────────────────────────────────────────
    {"key": "cid",                 "label": "PubChem CID",                "category": "Annotations",  "default": True},
    {"key": "chembl",              "label": "ChEMBL",                     "category": "Annotations",  "default": True},
    {"key": "chebi",               "label": "ChEBI",                      "category": "Annotations",  "default": True},
    {"key": "unii",                "label": "FDA UNII Code",              "category": "Annotations",  "default": True},
    {"key": "cas",                 "label": "CAS Registry Number",        "category": "Annotations",  "default": True},
    {"key": "pt",                  "label": "Preferred Term (FDA)",       "category": "Annotations",  "default": True},
    {"key": "names",               "label": "Names (FDA)",                "category": "Annotations",  "default": True},
    {"key": "devphase",            "label": "Highest Development Phase",  "category": "Annotations",  "default": True},
    {"key": "description",         "label": "Substance Description",      "category": "Annotations",  "default": True},
    {"key": "moa",                 "label": "Substance primary MOA",      "category": "Annotations",  "default": False},
    {"key": "drugbankmoa",         "label": "DrugBank primary MOA",       "category": "Annotations",  "default": False},
    {"key": "npcmoa",              "label": "NPC primary MOA",            "category": "Annotations",  "default": False},
    {"key": "ptargets",            "label": "Primary Targets",            "category": "Annotations",  "default": False},
    {"key": "stitch_target_human", "label": "Target (Human) geneIDs",     "category": "Annotations",  "default": False},
    {"key": "stitch_target",       "label": "Target geneIDs",             "category": "Annotations",  "default": False},
    {"key": "predictor",           "label": "NCATS Predictor Models",     "category": "Annotations",  "default": False},
    {"key": "predictorTarget",     "label": "NCATS Predictor Target",     "category": "Annotations",  "default": False},
    # ── Calculations ─────────────────────────────────────────────────
    {"key": "cns",                 "label": "CNS Score",                  "category": "Calculations", "default": True},
    {"key": "drug",                "label": "Drug-like Violations",       "category": "Calculations", "default": True},
    {"key": "hbd",                 "label": "Hydrogen Bond Donors",       "category": "Calculations", "default": True},
    {"key": "hba",                 "label": "Hydrogen Bond Acceptors",    "category": "Calculations", "default": True},
    {"key": "tpsa",                "label": "TPSA",                       "category": "Calculations", "default": True},
    {"key": "logp",                "label": "LogP",                       "category": "Calculations", "default": True},
    {"key": "logd",                "label": "LogD",                       "category": "Calculations", "default": True},
    {"key": "lead",                "label": "Lead-like Violations",       "category": "Calculations", "default": False},
    {"key": "mbpka",               "label": "Most Basic pKa",             "category": "Calculations", "default": False},
    {"key": "fastmap",             "label": "FASTMAP Embedding",          "category": "Calculations", "default": False},
    # ── Structure ────────────────────────────────────────────────────
    {"key": "smiles",              "label": "SMILES",                     "category": "Structure",    "default": True},
    {"key": "molForm",             "label": "Molecular Formula",          "category": "Structure",    "default": True},
    {"key": "molWeight",           "label": "Molecular Weight",           "category": "Structure",    "default": True},
    {"key": "inchikey",            "label": "InChIKey",                   "category": "Structure",    "default": False},
    {"key": "img",                 "label": "Structure Image URL",        "category": "Structure",    "default": False},
    {"key": "smilesParent",        "label": "SMILES [Parent]",            "category": "Structure",    "default": False},
    {"key": "molFormParent",       "label": "Molecular Formula [Parent]", "category": "Structure",    "default": False},
    {"key": "molWeightParent",     "label": "Molecular Weight [Parent]",  "category": "Structure",    "default": False},
    {"key": "molExactMass",        "label": "Exact Molecular Mass",       "category": "Structure",    "default": False},
    {"key": "molExactMassParent",  "label": "Exact Molecular Mass [Parent]", "category": "Structure", "default": False},
    {"key": "lychi",               "label": "Lychi Hash",                 "category": "Structure",    "default": False},
    {"key": "lychiParent",         "label": "Lychi Hash [Parent]",        "category": "Structure",    "default": False},
    {"key": "hash",                "label": "Layered Hashes",             "category": "Structure",    "default": False},
    {"key": "rbc",                 "label": "Rotatable Bonds",            "category": "Structure",    "default": False},
    {"key": "stereoCount",         "label": "Stereocenter Count",         "category": "Structure",    "default": False},
    {"key": "stereoParent",        "label": "Stereo [Parent]",            "category": "Structure",    "default": False},
    {"key": "pains",               "label": "PAINS Filters",              "category": "Structure",    "default": False},
    {"key": "sp3f",                "label": "SP3 Carbon Fraction",        "category": "Structure",    "default": False},
    {"key": "sp3sp2f",             "label": "SP3/(SP3+SP2) Carbon Fraction", "category": "Structure", "default": False},
    {"key": "sssr",                "label": "SSSR Count",                 "category": "Structure",    "default": False},
    {"key": "tautomers",           "label": "Tautomers",                  "category": "Structure",    "default": False},
    {"key": "smallestMoietySmiles","label": "Salt SMILES",                "category": "Structure",    "default": False},
    {"key": "smallestMoietyName",  "label": "Salt Name",                  "category": "Structure",    "default": False},
    {"key": "smallestMoietyCount", "label": "Salt Count",                 "category": "Structure",    "default": False},
]

NCATS_DEFAULT_PROPS: list[str] = [p["key"] for p in NCATS_PROPERTY_CATALOG if p["default"]]
NCATS_RESOLUTION_FALLBACK_PROPS: list[str] = [
    "cid", "chembl", "chebi", "unii", "cas", "pt", "names", "devphase", "description",
]

# Lookup: key → catalog entry
_NCATS_CATALOG_BY_KEY: dict[str, dict[str, Any]] = {p["key"]: p for p in NCATS_PROPERTY_CATALOG}
# All valid keys
_NCATS_VALID_KEYS: set[str] = {p["key"] for p in NCATS_PROPERTY_CATALOG}


def get_ncats_property_catalog() -> list[dict[str, Any]]:
    """Return the full NCATS property catalog for the frontend."""
    return NCATS_PROPERTY_CATALOG

CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")
_INTERNAL_ID_RE = re.compile(r"^IFX[A-Za-z]+:")  # skip internal IDs for external API lookups
_NCATS_RESOLVER_MAX_CONCURRENT = 1
_NCATS_RESOLVER_RETRY_ATTEMPTS = 2
_NCATS_RESOLVER_RETRY_BACKOFF_SECONDS = 0.25
_NCATS_RESOLVER_SEMAPHORE = BoundedSemaphore(_NCATS_RESOLVER_MAX_CONCURRENT)

logger = logging.getLogger(__name__)

_DRUG_SALT_SUFFIXES = (
    "hydrochloride",
    "hcl",
    "mesylate",
    "sulfate",
    "sulphate",
    "sodium",
    "potassium",
    "phosphate",
    "acetate",
    "citrate",
    "fumarate",
    "maleate",
    "tartrate",
    "succinate",
    "besylate",
    "tosylate",
    "bromide",
    "chloride",
    "calcium",
    "magnesium",
)

_DRUG_FORM_WORDS = {
    "tablet",
    "tablets",
    "capsule",
    "capsules",
    "injection",
    "injectable",
    "solution",
    "suspension",
    "powder",
    "concentrate",
    "vial",
    "kit",
}

_TIER_SORT_WEIGHT = {
    "high_confidence_identity": 0.30,
    "source_standard_multi_source_identity": 0.22,
    "source_standard_single_source_identity": 0.12,
    "source_standard_conflict": 0.08,
}

# ── HTTP session ─────────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s = requests.Session()
    s.trust_env = False
    s.verify = False
    retry = Retry(
        total=0, connect=0, read=0, status=0, backoff_factor=0,
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


def _record_source_error(
    source_errors: list[dict[str, str]] | None,
    source: str,
    lookup: Any,
    message: Any,
) -> None:
    """Attach compact live-source diagnostics without turning no-hit responses into errors."""
    if source_errors is None or len(source_errors) >= 20:
        return
    lookup_text = str(lookup or "").strip()
    message_text = _shorten(message, 240) or "request failed"
    if any(err.get("source") == source and err.get("lookup") == lookup_text for err in source_errors):
        return
    entry = {"source": source, "lookup": lookup_text, "message": message_text}
    source_errors.append(entry)


def _append_source_failure(
    source_errors: list[dict[str, str]],
    source: str,
    attempt_errors: list[dict[str, str]],
) -> None:
    """Collapse per-lookup failures into one per-source diagnostic for a result."""
    if not attempt_errors:
        return
    lookups = _uniq_join([err.get("lookup") for err in attempt_errors], limit=4)
    messages = _uniq_join([err.get("message") for err in attempt_errors], limit=2)
    extra = "" if len(attempt_errors) <= 4 else f"; {len(attempt_errors) - 4} more attempts"
    source_errors.append({
        "source": source,
        "lookup": lookups,
        "message": f"{len(attempt_errors)} lookup attempt(s) failed{extra}: {messages}",
    })


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


def _split_pipe_values(value: Any, limit: int = 6) -> list[str]:
    """Split compact graph cells like CHEMBL1|CHEMBL2 into API lookup values."""
    values: list[str] = []
    for part in re.split(r"[|;,]", str(value or "")):
        part = part.strip()
        if part and part not in values:
            values.append(part)
    return values[:limit]


def _add_lookup_candidate(candidates: list[str], seen: set[str], value: Any) -> None:
    text = _clean_query_fragment(value)
    if len(text) < 2 or _INTERNAL_ID_RE.match(text):
        return
    key = text.lower()
    if key in seen:
        return
    seen.add(key)
    candidates.append(text)


def _identifier_lookup_variants(value: Any) -> list[str]:
    """Return resolver-friendly variants for local graph identifier cells."""
    text = _clean_query_fragment(value)
    if not text:
        return []
    variants = [text]
    if ":" not in text:
        return variants

    prefix, local_id = text.split(":", 1)
    prefix_key = prefix.strip().lower()
    local_id = local_id.strip()
    if not local_id:
        return variants
    if prefix_key in {
        "unii",
        "chembl",
        "chembl.compound",
        "pubchem",
        "pubchem.compound",
        "cas",
        "inchikey",
    }:
        variants.append(local_id)
    elif prefix_key == "chebi":
        variants.append(f"CHEBI:{local_id}")
    return variants


def _add_identifier_lookup_candidates(
    candidates: list[str],
    seen: set[str],
    value: Any,
    limit: int = 4,
) -> None:
    for item in _split_pipe_values(value, limit=limit):
        for variant in _identifier_lookup_variants(item):
            _add_lookup_candidate(candidates, seen, variant)


def _add_name_lookup_candidates(candidates: list[str], seen: set[str], value: Any) -> None:
    text = _clean_query_fragment(value)
    if not text:
        return
    for variant in (text, _strip_form_words(text), _strip_salt_or_suffix(text)):
        _add_lookup_candidate(candidates, seen, variant)


def _ncats_lookup_candidates(
    query: str,
    name: str,
    best: dict[str, Any],
    query_terms: list[str] | None = None,
) -> list[str]:
    """Build robust NCATS lookups from user text plus harmonized identifiers."""
    candidates: list[str] = []
    seen: set[str] = set()

    _add_name_lookup_candidates(candidates, seen, query)
    for term in (query_terms or [])[:4]:
        _add_name_lookup_candidates(candidates, seen, term)
    _add_name_lookup_candidates(candidates, seen, name)

    for field, limit in (
        ("unii", 4),
        ("inchikey", 2),
        ("chembl_id", 4),
        ("chebi_id", 2),
        ("cas", 2),
        ("pubchem_cid", 2),
        ("source_standard_primary_id", 2),
        ("primary_id", 2),
        ("nodenorm_canonical_curie", 1),
    ):
        _add_identifier_lookup_candidates(candidates, seen, best.get(field), limit=limit)

    return candidates[:14]


def _clean_query_fragment(value: Any) -> str:
    text = unescape(str(value or ""))
    text = re.sub(r"[\u2010-\u2015\u2212]", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[\s,;:/+&()\\[\\]-]+|[\s,;:/+&()\\[\\]-]+$", "", text)
    return text.strip()


def _normalize_drug_text(value: Any, keep_bracket_text: bool = False) -> str:
    text = _clean_query_fragment(value).lower()
    if keep_bracket_text:
        text = re.sub(r"[\[\]{}()]", " ", text)
    else:
        text = re.sub(r"\[[^\]]*\]|\([^)]*\)", " ", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _strip_form_words(value: str) -> str:
    parts = _clean_query_fragment(value).split()
    while parts and parts[-1].lower() in _DRUG_FORM_WORDS:
        parts.pop()
    return " ".join(parts).strip()


def _strip_salt_or_suffix(value: str) -> str:
    text = _strip_form_words(value)
    lower = text.lower()
    for suffix in _DRUG_SALT_SUFFIXES:
        marker = f" {suffix}"
        if lower.endswith(marker) and len(text) > len(marker) + 2:
            return text[: -len(marker)].strip()
    stripped = re.sub(r"\s*-\s*[a-z]{4}$", "", text, flags=re.I).strip()
    if stripped and stripped != text:
        return stripped
    return text


def _split_drug_terms(value: str) -> list[str]:
    text = _clean_query_fragment(value)
    if not text:
        return []
    pieces = re.split(r"\s+(?:and|or)\s+|[;&/+]", text, flags=re.I)
    return [_clean_query_fragment(piece) for piece in pieces if _clean_query_fragment(piece)]


def _drug_query_candidates(query: str) -> list[str]:
    """Return resolver lookup terms for labels like "generic [Brand]".

    The resolver keeps the original query, but also searches the pre-bracket
    generic name, bracketed brand names, split combination terms, salt-stripped
    forms, and biologic-suffix-stripped forms.
    """
    seen: set[str] = set()
    terms: list[str] = []

    def add(value: Any) -> None:
        text = _clean_query_fragment(value)
        if not text:
            return
        variants = [text, _strip_form_words(text), _strip_salt_or_suffix(text)]
        for variant in variants:
            variant = _clean_query_fragment(variant)
            if len(variant) < 2:
                continue
            key = variant.lower()
            if key not in seen:
                seen.add(key)
                terms.append(variant)

    raw = _clean_query_fragment(query)
    if not raw:
        return []

    add(raw)
    bracket_parts = [m.group(1) or m.group(2) for m in re.finditer(r"\[([^\]]+)\]|\(([^)]+)\)", raw)]
    outside = re.sub(r"\[[^\]]+\]|\([^)]*\)", " ", raw)
    add(outside)
    for piece in _split_drug_terms(outside):
        add(piece)
    for bracket in bracket_parts:
        add(bracket)
        for piece in _split_drug_terms(bracket):
            add(piece)

    normalized_with_brackets = _normalize_drug_text(raw, keep_bracket_text=True)
    normalized_without_brackets = _normalize_drug_text(raw, keep_bracket_text=False)
    add(normalized_with_brackets)
    add(normalized_without_brackets)
    return terms[:24]


def _node_search_values(node: dict[str, str]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for field in (
        "drug_id", "standard_name", "primary_id", "nodenorm_canonical_label",
        "unii", "chembl_id", "chebi_id", "pubchem_cid", "drugcentral_id",
        "rxcui", "cas", "inchikey",
    ):
        value = str(node.get(field, "") or "").strip()
        if value:
            values.append((field, value))
    for field in ("synonyms", "source_ids", "xrefs"):
        for value in str(node.get(field, "") or "").split("|"):
            value = value.strip()
            if value:
                values.append((field, value))
    return values


def _term_value_score(query_norm: str, value_norm: str) -> float:
    if not query_norm or not value_norm:
        return 0.0
    if query_norm == value_norm:
        return 1.0
    score = 0.0
    if len(query_norm) >= 5 and query_norm in value_norm:
        score = max(score, 0.95 if value_norm.startswith(query_norm) else 0.91)
    if len(value_norm) >= 5 and value_norm in query_norm:
        score = max(score, 0.88)
    q_tokens = set(query_norm.split())
    v_tokens = set(value_norm.split())
    if q_tokens and v_tokens:
        overlap = len(q_tokens & v_tokens)
        min_tokens = min(len(q_tokens), len(v_tokens))
        max_tokens = max(len(q_tokens), len(v_tokens))
        containment = overlap / min_tokens
        coverage = overlap / max_tokens
        if containment >= 0.80 and (len(q_tokens) > 1 or len(v_tokens) > 1):
            score = max(score, 0.80 + (0.12 * containment) + (0.04 * coverage))
    if len(query_norm) >= 5 and len(value_norm) >= 5:
        score = max(score, 0.82 * SequenceMatcher(None, query_norm, value_norm).ratio())
    return min(score, 1.0)


def _score_node_against_terms(node: dict[str, str], terms: list[str]) -> tuple[float, str, str, str]:
    best = (0.0, "", "", "")
    norm_terms = [(term, _normalize_drug_text(term, keep_bracket_text=True)) for term in terms]
    for field, value in _node_search_values(node):
        value_norm = _normalize_drug_text(value, keep_bracket_text=True)
        if not value_norm:
            continue
        for term, term_norm in norm_terms:
            score = _term_value_score(term_norm, value_norm)
            if score > best[0]:
                best = (score, term, field, value)
                if score >= 1.0:
                    return best
    return best


def _floatish(value: Any) -> float:
    try:
        return float(str(value or "0").split("|")[0])
    except ValueError:
        return 0.0


def _rank_local_hit(row: dict[str, Any]) -> tuple[float, float, float, float]:
    score = _floatish(row.get("_match_score"))
    tier = _TIER_SORT_WEIGHT.get(str(row.get("evidence_tier", "") or "").strip(), 0.0)
    support = _floatish(row.get("source_support"))
    source_records = _floatish(row.get("source_record_count"))
    return (score, tier, support, source_records)


def _resolve_local_hits(data: DrugGraphData, query: str, limit: int = 10) -> tuple[list[dict[str, Any]], list[str], int]:
    terms = _drug_query_candidates(query)
    if not terms:
        return [], [], 0

    candidates: dict[str, dict[str, Any]] = {}

    def add_hit(drug_id: str, term: str, strategy: str, base_score: float = 0.0) -> None:
        node = data.nodes_by_id.get(drug_id)
        if not node:
            return
        score, matched_term, matched_field, matched_value = _score_node_against_terms(node, [term])
        score = max(score, base_score)
        existing = candidates.get(drug_id)
        if existing and _floatish(existing.get("_match_score")) >= score:
            return
        hit = dict(node)
        hit["_match_score"] = f"{score:.3f}"
        hit["_matched_query"] = matched_term or term
        hit["_matched_field"] = matched_field
        hit["_matched_value"] = matched_value
        hit["_match_strategy"] = strategy
        candidates[drug_id] = hit

    for term in terms:
        lookup_keys = {
            term,
            term.lower(),
            term.upper(),
            _normalize_drug_text(term, keep_bracket_text=True),
            _normalize_drug_text(term, keep_bracket_text=False),
        }
        exact_term_hit = False
        for key in lookup_keys:
            exact_ids = data.ids_to_drugs.get(key, [])
            if exact_ids:
                exact_term_hit = True
            for drug_id in exact_ids:
                add_hit(drug_id, key if key != term else term, "exact_alias", 1.0)

        if exact_term_hit:
            continue

        payload = search_drugs(data, q=term, per_page=25)
        for node in payload.get("rows", []):
            drug_id = node.get("drug_id")
            if drug_id:
                add_hit(drug_id, term, "substring")

    if not candidates:
        for node in data.nodes:
            score, matched_term, matched_field, matched_value = _score_node_against_terms(node, terms)
            if score >= 0.86:
                hit = dict(node)
                hit["_match_score"] = f"{score:.3f}"
                hit["_matched_query"] = matched_term
                hit["_matched_field"] = matched_field
                hit["_matched_value"] = matched_value
                hit["_match_strategy"] = "fuzzy_lexical"
                candidates[node.get("drug_id", "")] = hit

    ranked = sorted(candidates.values(), key=_rank_local_hit, reverse=True)
    return ranked[:limit], terms, len(ranked)


def _compact_local_hit(hit: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "drug_id",
        "primary_id",
        "source_standard_primary_id",
        "standard_name",
        "definition",
        "biolink_category",
        "drug_scope",
        "source_namespaces",
        "nodenorm_canonical_curie",
        "nodenorm_canonical_label",
        "nodenorm_validation_status",
        "inchikey",
        "unii",
        "pubchem_cid",
        "chembl_id",
        "chebi_id",
        "drugcentral_id",
        "rxcui",
        "cas",
        "atc_codes",
        "approval_status",
        "source_record_count",
        "source_support",
        "structural_key_type",
        "evidence_tier",
        "quality_flags",
        "_match_score",
        "_matched_query",
        "_matched_field",
        "_matched_value",
        "_match_strategy",
    ]
    compact = {field: hit.get(field, "") for field in fields if hit.get(field, "")}
    if len(str(compact.get("definition", ""))) > 220:
        compact["definition"] = _shorten(compact["definition"], 220) or ""
    if len(str(compact.get("_matched_value", ""))) > 220:
        compact["_matched_value"] = _shorten(compact["_matched_value"], 220) or ""
    return compact


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
        hits, query_terms, total = _resolve_local_hits(data, q, limit=5)
        results.append({
            "query": q,
            "query_terms": query_terms,
            "local_hits": [_compact_local_hit(hit) for hit in hits],
            "resolved": len(hits) > 0,
            "total_local_matches": total,
        })
    return results


# ═════════════════════════════════════════════════════════════════════════
# STEP 2a: NCATS RESOLVER
# ═════════════════════════════════════════════════════════════════════════

def enrich_ncats_resolver(
    query: str,
    session: requests.Session | None = None,
    api_key: str = "5fd5bb2a05eb6195",
    timeout: int = 6,
    props: list[str] | None = None,
    source_errors: list[dict[str, str]] | None = None,
) -> dict[str, Any] | None:
    """Query the NCATS chemical resolver for a single identifier.

    *props* selects which NCATS properties to retrieve.  Defaults to
    ``NCATS_DEFAULT_PROPS`` when ``None``.  The returned dict uses
    ``ncats_<api_key>`` keys for every requested property that has a
    value, plus convenience keys ``ncats_resolved_name`` and
    ``ncats_all_names`` when "names" is in the property list.
    """
    session = session or _build_session()
    prop_list = list(props) if props else list(NCATS_DEFAULT_PROPS)
    # Validate — silently drop unknown keys
    prop_list = [p for p in prop_list if p in _NCATS_VALID_KEYS]
    if not prop_list:
        return None

    fallback_props = [p for p in NCATS_RESOLUTION_FALLBACK_PROPS if p in prop_list]
    request_groups: list[tuple[list[str], bool]] = [(prop_list, False)]
    if fallback_props and any(p not in fallback_props for p in prop_list):
        request_groups.append((fallback_props, True))

    def parse_response(text: str, active_props: list[str], used_fallback: bool) -> dict[str, Any] | None:
        text = text.strip()
        if not text:
            return None
        values = text.split("\n")[0].split("\t")
        if len(values) < 2:
            return None
        raw: dict[str, str] = {}
        for i, prop in enumerate(active_props):
            idx = i + 1
            if idx < len(values):
                v = values[idx].strip()
                if v and v != "0":
                    v = re.sub(r"^\[NO STEREO\]\s*", "", v)
                    raw[prop] = v
        if not raw:
            return None

        result: dict[str, Any] = {}
        for prop in active_props:
            val = raw.get(prop)
            if val is not None:
                if prop == "description":
                    result[f"ncats_{prop}"] = _shorten(val, 300)
                else:
                    result[f"ncats_{prop}"] = val

        names_raw = raw.get("names", "")
        name_list = [n.strip() for n in names_raw.split("|") if n.strip()]
        if name_list:
            result["ncats_resolved_name"] = name_list[0]
            result["ncats_all_names"] = "|".join(name_list[:15])

        result["_ncats_props_requested"] = active_props
        if used_fallback:
            result["ncats_request_note"] = "Full property request failed; returned identifier/name fallback."
        return result or None

    last_error: Any = None
    for active_props, used_fallback in request_groups:
        url = f"{NCATS_RESOLVER_BASE}/{'/'.join(active_props)}/"
        params = {
            "structure": query,
            "standardize": "CHARGE_NORMALIZE",
            "force": "false",
            "apikey": api_key,
            "useApproxMatch": "false",
            "useContains": "false",
        }
        for attempt in range(1, _NCATS_RESOLVER_RETRY_ATTEMPTS + 1):
            try:
                with _NCATS_RESOLVER_SEMAPHORE:
                    r = session.get(url, params=params, timeout=timeout)
                if r.status_code != 200:
                    last_error = f"HTTP {r.status_code}"
                    if (
                        r.status_code in {408, 425, 429, 500, 502, 503, 504}
                        and attempt < _NCATS_RESOLVER_RETRY_ATTEMPTS
                    ):
                        time.sleep(_NCATS_RESOLVER_RETRY_BACKOFF_SECONDS * attempt)
                        continue
                    break
                result = parse_response(r.text, active_props, used_fallback)
                if result:
                    return result
                return None
            except Exception as exc:
                last_error = exc
                if attempt < _NCATS_RESOLVER_RETRY_ATTEMPTS:
                    time.sleep(_NCATS_RESOLVER_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                logger.debug("NCATS resolver failed for %s: %s", query, exc)
                break

    if last_error is not None:
        _record_source_error(source_errors, "ncats_resolver", query, last_error)
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
    timeout: int = 5,
    source_errors: list[dict[str, str]] | None = None,
) -> dict[str, Any] | None:
    """Query Pharos GraphQL for drug-target activities."""
    if not name:
        return None
    session = session or _build_session()

    # Try cleaned name first, then raw
    for lookup in list(dict.fromkeys(filter(None, [_clean_name_for_pharos(name), name]))):
        if not lookup:
            continue
        try:
            r = session.post(
                PHAROS_API,
                json={"query": _PHAROS_QUERY, "variables": {"ligid": lookup}},
                timeout=timeout,
            )
            if r.status_code != 200:
                _record_source_error(source_errors, "pharos", lookup, f"HTTP {r.status_code}")
                continue
            data = r.json()
            if data.get("errors"):
                messages = [
                    str(err.get("message", err))
                    for err in data.get("errors") or []
                ]
                _record_source_error(source_errors, "pharos", lookup, "; ".join(messages))
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
            _record_source_error(source_errors, "pharos", lookup, exc)
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
    timeout: int = 6,
    source_errors: list[dict[str, str]] | None = None,
) -> dict[str, Any] | None:
    """Query Inxight/GSRS by UNII for regulatory + target context."""
    if not unii:
        return None
    session = session or _build_session()
    unii = re.sub(r"^\[NO STEREO\]\s*", "", str(unii).split("|")[0].strip()).strip()
    if not unii:
        return None

    # API search
    try:
        r = session.get(f"{INXIGHT_API}/search", params={"q": unii}, timeout=timeout)
        if r.status_code != 200:
            _record_source_error(source_errors, "inxight", unii, f"HTTP {r.status_code}")
            return None
        hits = (r.json().get("content") or [])
        if not hits:
            return None
        d = next((h for h in hits if str(h.get("approvalID", "")).upper() == unii.upper()), hits[0])
        uuid = d.get("uuid")
        if not uuid:
            return None

        result: dict[str, Any] = {"inxight_drug_url": f"https://drugs.ncats.io/drug/{unii}"}

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
        _record_source_error(source_errors, "inxight", unii, exc)
        logger.debug("Inxight failed for %s: %s", unii, exc)
        return result or None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2d: openFDA
# ═════════════════════════════════════════════════════════════════════════

def enrich_openfda(
    name_candidates: list[str],
    session: requests.Session | None = None,
    timeout: int = 3,
    source_errors: list[dict[str, str]] | None = None,
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
                    if resp.status_code == 429 or resp.status_code >= 500:
                        _record_source_error(source_errors, "openfda", candidate, f"HTTP {resp.status_code}")
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
            except Exception as exc:
                _record_source_error(source_errors, "openfda", candidate, exc)
                continue
    return None


# ═════════════════════════════════════════════════════════════════════════
# STEP 2e: ChEBI OLS
# ═════════════════════════════════════════════════════════════════════════

def enrich_chebi(
    chebi_id: str,
    session: requests.Session | None = None,
    timeout: int = 3,
    source_errors: list[dict[str, str]] | None = None,
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
            if term_resp.status_code == 429 or term_resp.status_code >= 500:
                _record_source_error(source_errors, "chebi", chebi_id, f"HTTP {term_resp.status_code}")
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
        _record_source_error(source_errors, "chebi", chebi_id, exc)
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
    graph_data: "DrugGraphData | None" = None,
    ncats_props: list[str] | None = None,
) -> dict[str, Any]:
    """Enrich a single local-resolved result with live API data."""
    session = _build_session()
    enrichment: dict[str, Any] = {}
    source_errors: list[dict[str, str]] = []
    query = local_result["query"]
    hits = local_result.get("local_hits") or []
    best = hits[0] if hits else {}

    # Determine lookup keys from local hit or raw query
    name = best.get("standard_name") or query
    unii_values = _split_pipe_values(best.get("unii"))
    chebi_values = _split_pipe_values(best.get("chebi_id"))
    chembl_values = _split_pipe_values(best.get("chembl_id"))
    unii = unii_values[0] if unii_values else ""
    chebi_id = chebi_values[0] if chebi_values else ""
    chembl_id = chembl_values[0] if chembl_values else ""

    # NCATS Resolver — try original query FIRST so a user-typed name
    # (e.g. "prednisolone") isn't overshadowed by a fuzzy local-graph hit
    # (e.g. Cortisol which has "PREDNISOLONE IMPURITY A" as a synonym).
    if enable_ncats:
        ncats_attempt_errors: list[dict[str, str]] = []
        seen_lookups: set[str] = set()
        ncats_lookups = _ncats_lookup_candidates(
            query,
            name,
            best,
            query_terms=local_result.get("query_terms") or [],
        )
        for lookup in ncats_lookups:
            if not lookup or lookup in seen_lookups or _INTERNAL_ID_RE.match(lookup):
                continue
            seen_lookups.add(lookup)
            ncats = enrich_ncats_resolver(
                lookup,
                session=session,
                props=ncats_props,
                source_errors=ncats_attempt_errors,
            )
            if ncats:
                enrichment["ncats_resolver"] = ncats
                # Override IDs with NCATS-resolved values so downstream
                # lookups use the correct compound, not the local hit
                if ncats.get("ncats_unii"):
                    unii_values = _split_pipe_values(ncats["ncats_unii"]) + [
                        v for v in unii_values if v not in _split_pipe_values(ncats["ncats_unii"])
                    ]
                    unii = unii_values[0] if unii_values else ""
                if ncats.get("ncats_chebi"):
                    chebi_values = _split_pipe_values(ncats["ncats_chebi"]) + [
                        v for v in chebi_values if v not in _split_pipe_values(ncats["ncats_chebi"])
                    ]
                    chebi_id = chebi_values[0] if chebi_values else ""
                if ncats.get("ncats_chembl"):
                    chembl_values = _split_pipe_values(ncats["ncats_chembl"]) + [
                        v for v in chembl_values if v not in _split_pipe_values(ncats["ncats_chembl"])
                    ]
                    chembl_id = chembl_values[0] if chembl_values else ""
                resolved_name = ncats.get("ncats_resolved_name")
                if resolved_name:
                    name = resolved_name
                break
            time.sleep(delay)
        if "ncats_resolver" not in enrichment:
            _append_source_failure(source_errors, "ncats_resolver", ncats_attempt_errors)

        # Re-lookup in local graph using NCATS-resolved IDs when the
        # original query missed or matched the wrong compound (e.g.
        # NCGC00256577-01 → prednisolone UNII 9PHQ9Y1OLM → IFXDrug).
        if graph_data and enrichment.get("ncats_resolver"):
            ncats_ids = enrichment["ncats_resolver"]
            relookup_terms = [v for v in [
                *_split_pipe_values(ncats_ids.get("ncats_unii")),
                *_split_pipe_values(ncats_ids.get("ncats_chembl")),
                *_split_pipe_values(ncats_ids.get("ncats_chebi")),
                ncats_ids.get("ncats_resolved_name"),
            ] if v]
            # Only re-lookup if the original local hit seems wrong or absent
            need_relookup = (
                not best  # no local hit at all
                or (best.get("unii") and ncats_ids.get("ncats_unii")
                    and best["unii"] != ncats_ids["ncats_unii"])  # wrong compound
            )
            if need_relookup and relookup_terms:
                for term in relookup_terms:
                    retry = resolve_local(graph_data, [term])
                    retry_hits = (retry[0].get("local_hits") or []) if retry else []
                    if retry_hits:
                        local_result = {
                            **local_result,
                            "local_hits": retry_hits,
                            "resolved": True,
                            "total_local_matches": retry[0].get("total_local_matches", len(retry_hits)),
                            "resolved_via": f"NCATS→{term}",
                        }
                        best = retry_hits[0]
                        break

    # Pharos — try original query first, then resolved name, then ChEMBL
    if enable_pharos:
        pharos = None
        pharos_attempt_errors: list[dict[str, str]] = []
        seen_pharos: set[str] = set()
        for lookup in [query, name, *chembl_values]:
            if not lookup or lookup in seen_pharos or _INTERNAL_ID_RE.match(lookup):
                continue
            seen_pharos.add(lookup)
            pharos = enrich_pharos(lookup, session=session, source_errors=pharos_attempt_errors)
            if pharos:
                break
        if pharos:
            enrichment["pharos"] = pharos
        else:
            _append_source_failure(source_errors, "pharos", pharos_attempt_errors)
        time.sleep(delay)

    # Inxight
    if enable_inxight and unii_values:
        inxight_attempt_errors: list[dict[str, str]] = []
        for lookup in unii_values[:1]:
            inxight = enrich_inxight(lookup, session=session, source_errors=inxight_attempt_errors)
            if inxight:
                enrichment["inxight"] = inxight
                # Collect name candidates for openFDA
                if not name or name == query:
                    name = inxight.get("inxight_name") or name
                break
            time.sleep(delay)
        if "inxight" not in enrichment:
            _append_source_failure(source_errors, "inxight", inxight_attempt_errors)

    # ChEBI
    if enable_chebi and chebi_values:
        chebi_attempt_errors: list[dict[str, str]] = []
        for lookup in chebi_values[:2]:
            chebi = enrich_chebi(lookup, session=session, source_errors=chebi_attempt_errors)
            if chebi:
                enrichment["chebi"] = chebi
                break
            time.sleep(delay)
        if "chebi" not in enrichment:
            _append_source_failure(source_errors, "chebi", chebi_attempt_errors)

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
            openfda_attempt_errors: list[dict[str, str]] = []
            openfda = enrich_openfda(name_candidates, session=session, source_errors=openfda_attempt_errors)
            if openfda:
                enrichment["openfda"] = openfda
            else:
                _append_source_failure(source_errors, "openfda", openfda_attempt_errors)

    if source_errors:
        enrichment["source_errors"] = source_errors
        enrichment["sources_failed"] = sorted({err["source"] for err in source_errors if err.get("source")})

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
    ncats_props: list[str] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Full resolve+enrich for a batch of user queries.

    Returns a dict with ``results`` (list, one per query) and ``stats``.
    """
    def emit_progress(event: dict[str, Any]) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(event)
        except Exception:
            logger.debug("Drug resolver progress callback failed", exc_info=True)

    # Step 1: local resolution (fast, no API)
    local_results = resolve_local(data, queries)
    local_resolved = sum(1 for r in local_results if r.get("resolved"))
    emit_progress({
        "stage": "local_complete",
        "completed": 0,
        "total": len(local_results),
        "local_resolved": local_resolved,
        "message": f"Local graph resolved {local_resolved} of {len(local_results)} queries.",
    })

    # If no enrichment sources enabled, return local-only
    any_enrichment = any([enable_ncats, enable_pharos, enable_inxight, enable_openfda, enable_chebi])
    if not any_enrichment:
        emit_progress({
            "stage": "complete",
            "completed": len(local_results),
            "total": len(local_results),
            "local_resolved": local_resolved,
            "message": "Local-only resolution complete.",
        })
        return {
            "results": local_results,
            "stats": {
                "total": len(queries),
                "local_resolved": local_resolved,
                "enrichment_enabled": False,
            },
        }

    # Step 2: threaded enrichment
    enriched: list[dict[str, Any]] = [{}] * len(local_results)
    stats_lock = Lock()
    stats = {
        "total": len(queries),
        "local_resolved": local_resolved,
        "ncats_found": 0,
        "pharos_found": 0,
        "inxight_found": 0,
        "chebi_found": 0,
        "openfda_found": 0,
        "source_errors": 0,
        "sources_failed": {},
        "enrichment_enabled": True,
    }
    source_stat_keys = {
        "ncats_resolver": "ncats_found",
        "pharos": "pharos_found",
        "inxight": "inxight_found",
        "chebi": "chebi_found",
        "openfda": "openfda_found",
    }
    completed = 0

    def worker(idx: int, local_result: dict) -> tuple[int, dict]:
        nonlocal completed
        result = _enrich_one(
            local_result,
            enable_ncats=enable_ncats,
            enable_pharos=enable_pharos,
            enable_inxight=enable_inxight,
            enable_openfda=enable_openfda,
            enable_chebi=enable_chebi,
            delay=delay,
            graph_data=data,
            ncats_props=ncats_props,
        )
        with stats_lock:
            found = (result.get("enrichment") or {}).get("sources_found") or []
            for src in found:
                key = source_stat_keys.get(src, f"{src}_found")
                if key in stats:
                    stats[key] += 1
            source_errors = (result.get("enrichment") or {}).get("source_errors") or []
            stats["source_errors"] += len(source_errors)
            failed_counts = stats["sources_failed"]
            for err in source_errors:
                src = err.get("source") or "unknown"
                failed_counts[src] = failed_counts.get(src, 0) + 1
            completed += 1
            emit_progress({
                "stage": "enriching",
                "completed": completed,
                "total": len(local_results),
                "local_resolved": stats["local_resolved"],
                "sources_found": {
                    key.replace("_found", ""): value
                    for key, value in stats.items()
                    if key.endswith("_found") and value
                },
                "sources_failed": dict(stats["sources_failed"]),
                "message": f"Enriched {completed} of {len(local_results)} resolved rows.",
            })
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
                with stats_lock:
                    completed += 1
                    emit_progress({
                        "stage": "enriching",
                        "completed": completed,
                        "total": len(local_results),
                        "local_resolved": stats["local_resolved"],
                        "sources_found": {
                            key.replace("_found", ""): value
                            for key, value in stats.items()
                            if key.endswith("_found") and value
                        },
                        "sources_failed": dict(stats["sources_failed"]),
                        "message": f"Enriched {completed} of {len(local_results)} resolved rows.",
                    })

    emit_progress({
        "stage": "complete",
        "completed": len(local_results),
        "total": len(local_results),
        "local_resolved": stats["local_resolved"],
        "sources_found": {
            key.replace("_found", ""): value
            for key, value in stats.items()
            if key.endswith("_found") and value
        },
        "sources_failed": dict(stats["sources_failed"]),
        "message": "Drug resolver enrichment complete.",
    })
    return {"results": enriched, "stats": stats}
