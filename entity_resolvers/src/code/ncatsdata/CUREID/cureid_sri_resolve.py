#!/usr/bin/env python3
"""
cureid_sri_resolve.py

Improved SRI Name Resolver for raw CUREID JSON:
- Adds biolink_type filtering per node_type
- Uses parenthetical cleanup + phenotype/AE chunking queries
- Treats existing_curie as PREMAPPED (skips SRI)
- Exact match checks label OR synonyms (case-insensitive)
- Keeps output contract: exactly two XLSX files in --outdir

USAGE
-----
python cureid_sri_resolve.py data/input/cureid_cases_12.16.25.json
python cureid_sri_resolve.py data/input/cureid_cases_12.16.25.json --outdir data/output
python cureid_sri_resolve.py data/input/cureid_cases_RASopathies_08.18.25.json --outdir data/output

OUTPUT (exactly two XLSX files in --outdir)
------------------------------------------
1) cureid_resolved_full_<TAG>.xlsx
2) SRI_nodes_non_exact_for_review_<TAG>.xlsx

NOTES
-----
- Extracts unique nodes from JSON edges using known fields (drug/disease/phenotype/etc.)
- Queries SRI per unique (original_node_label, node_type)
- Adds biolink_type filters and better exact-match logic (label or synonym)
- SequenceVariant rows are NOT sent to SRI and are forced UNMAPPABLE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

SRI_LOOKUP_URL = "https://name-resolution-sri.renci.org/lookup"

# ---------------------------
# Biolink type filters (port from your "better" script behavior)
# ---------------------------
NODETYPE_TO_BIOLINK_TYPES: Dict[str, List[str]] = {
    "Disease": ["biolink:Disease"],
    "Gene": ["biolink:Gene"],
    "Drug": [
        "biolink:Drug",
        "biolink:ChemicalSubstance",
        "biolink:SmallMolecule",
        "biolink:ChemicalEntity",
    ],
    # keep permissive; SRI often uses PhenotypicFeature for AE-like concepts
    "AdverseEvent": ["biolink:AdverseEvent", "biolink:PhenotypicFeature"],
    # if you want strict phenotype filtering, set ["biolink:PhenotypicFeature"]
    # but leaving empty is safer for recall across SRI variants
    "PhenotypicFeature": [],
}

# ---------------------------
# helpers
# ---------------------------

PAREN_RE = re.compile(r"\s*\([^)]*\)")

def is_blank(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    return s == "" or s.lower() in {"nan", "none", "null", "<na>"}


def normalize_text(x: Any) -> str:
    if is_blank(x):
        return ""
    return " ".join(str(x).strip().split()).lower()


def clean_name_for_sri(q: str) -> str:
    """
    - normalize whitespace
    - drop parentheticals (brand lists, clarifiers, etc.)
    """
    q = " ".join(str(q or "").strip().split())
    q = PAREN_RE.sub("", q).strip()
    return q


def chunk_feature_string(q: str) -> List[str]:
    """
    Split long phenotype/AE strings into manageable chunks for SRI.
    Return up to 3 candidates.
    """
    q = clean_name_for_sri(q)
    if is_blank(q):
        return []

    # split on common delimiters
    parts = re.split(r"[;|/]", q)
    parts = [" ".join(p.strip().split()) for p in parts if len(" ".join(p.strip().split())) >= 4]

    # fallback on commas if nothing found
    if not parts:
        parts = [" ".join(p.strip().split()) for p in q.split(",") if len(" ".join(p.strip().split())) >= 4]

    # de-dupe, keep reasonably short pieces
    uniq: List[str] = []
    seen = set()
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        if len(p) <= 80:
            uniq.append(p)

    if not uniq:
        uniq = [q[:120]]

    return uniq[:3]


def setup_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.headers.update({"Accept": "application/json"})
    return sess


def cache_key(query: str, biolink_types: List[str], max_hits: int, min_score: float = 0.0) -> str:
    s = f"{query.lower().strip()}|{','.join(sorted(biolink_types or []))}|{max_hits}|{min_score}"
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def sri_lookup(
    session: requests.Session,
    query: str,
    biolink_types: List[str],
    max_hits: int,
    min_score: float,
    delay_s: float,
    logger: logging.Logger,
    cache_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    SRI expects query param 'string'. We use GET for consistency.
    Adds optional repeated biolink_type params.
    Includes simple disk caching if cache_dir is provided.
    """
    q = clean_name_for_sri(query)
    if is_blank(q):
        return []

    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        ck = cache_key(q, biolink_types, max_hits, min_score)
        cfile = cache_dir / f"{ck}.json"
        if cfile.exists():
            try:
                return json.loads(cfile.read_text(encoding="utf-8"))
            except Exception:
                pass

    params_list: List[Tuple[str, str]] = [("string", q), ("limit", str(max_hits))]

    # If biolink_types is empty, we do not send a filter (more recall).
    for t in (biolink_types or []):
        params_list.append(("biolink_type", t))

    try:
        r = session.get(SRI_LOOKUP_URL, params=params_list, timeout=30)
    except Exception as e:
        logger.warning("SRI request failed for query=%r: %s", q, e)
        return []

    if r.status_code != 200:
        snippet = r.text[:300].replace("\n", " ")
        logger.warning("SRI HTTP %s for query %r: %s", r.status_code, q, snippet)
        return []

    if delay_s and delay_s > 0:
        time.sleep(delay_s)

    try:
        data = r.json()
    except Exception:
        logger.warning("SRI returned non-JSON for query=%r: %r", q, r.text[:250])
        return []

    hits: List[Dict[str, Any]] = []
    if isinstance(data, list):
        hits = data
    elif isinstance(data, dict) and isinstance(data.get("results"), list):
        hits = data["results"]

    # Filter by min_score first
    def score_of(h: Dict[str, Any]) -> float:
        try:
            return float(h.get("score", -1.0))
        except Exception:
            return -1.0
    
    # Apply min_score filter
    filtered_hits = [h for h in (hits or []) if score_of(h) >= min_score]

    # de-dupe by curie (prefer highest score)
    dedup: Dict[str, Dict[str, Any]] = {}
    for h in filtered_hits:
        curie = h.get("curie") or h.get("id") or h.get("identifier") or ""
        if is_blank(curie):
            continue
        curie = str(curie)
        if curie not in dedup or score_of(h) > score_of(dedup[curie]):
            dedup[curie] = h

    out = sorted(dedup.values(), key=score_of, reverse=True)[: max_hits]

    if cache_dir is not None:
        try:
            cfile.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    return out


def pick_best_hit(hits: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not hits:
        return None

    def score_of(h: Dict[str, Any]) -> float:
        try:
            return float(h.get("score", -1.0))
        except Exception:
            return -1.0

    return sorted(hits, key=score_of, reverse=True)[0]


def exact_match_against_label_or_synonyms(query: str, best_label: Any, synonyms: Any) -> bool:
    qn = normalize_text(query)
    if is_blank(qn) or is_blank(best_label):
        return False
    if qn == normalize_text(best_label):
        return True
    if isinstance(synonyms, list):
        for s in synonyms:
            if not is_blank(s) and qn == normalize_text(s):
                return True
    return False


def match_type(query: str, best_label: Any, synonyms: Any) -> str:
    if is_blank(best_label):
        return "UNRESOLVED"
    return "EXACT" if exact_match_against_label_or_synonyms(query, best_label, synonyms) else "NON_EXACT"


def safe_json_dumps(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return ""


def extract_label_and_curie(val: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    CUREID fields can be:
      - string label
      - dict like {"label": "...", "curie": "..."} or {"id": "...", "label": "..."}
      - list of dicts/strings (joined as literal with '|'; NOT split for node identity)
    Returns (label, existing_curie)
    """
    if is_blank(val):
        return None, None

    if isinstance(val, dict):
        label = val.get("label") or val.get("name") or val.get("text")
        curie = val.get("curie") or val.get("id") or val.get("identifier")
        if not is_blank(label):
            return str(label).strip(), (None if is_blank(curie) else str(curie).strip())
        if not is_blank(curie):
            s = str(curie).strip()
            return s, s
        return None, None

    if isinstance(val, list):
        parts: List[str] = []
        curies: List[str] = []
        for item in val:
            lab, cur = extract_label_and_curie(item)
            if not is_blank(lab):
                parts.append(str(lab).strip())
            if not is_blank(cur):
                curies.append(str(cur).strip())
        label = "|".join(parts) if parts else None
        curie = "|".join(curies) if curies else None
        return label, curie

    return str(val).strip(), None


def derive_tag_from_input(json_path: Path) -> str:
    """
    cureid_cases_12.16.25.json -> 12.16.25
    cureid_cases_RASopathies_08.18.25.json -> RASopathies_08.18.25
    otherwise -> stem
    """
    stem = json_path.stem
    m = re.match(r"^cureid_cases_(.+)$", stem)
    return m.group(1) if m else stem


# ---------------------------
# JSON parsing
# ---------------------------

DEFAULT_ENTITY_FIELDS: Dict[str, str] = {
    "drug": "Drug",
    "disease": "Disease",
    "phenotype": "PhenotypicFeature",
    "adverse_event": "AdverseEvent",
    "gene": "Gene",
    "protein_change": "SequenceVariant",
    "nucleotide_change": "SequenceVariant",
    "primary_target": "PhenotypicFeature",
    "secondary_target": "PhenotypicFeature",
}


def load_edges(input_json: Path) -> List[Dict[str, Any]]:
    with open(input_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("edges", "records", "items"):
            if isinstance(data.get(k), list):
                return data[k]
    raise ValueError("Unrecognized JSON structure. Expected list or dict with 'data' list.")


def build_nodes_table(edges: List[Dict[str, Any]], entity_fields: Dict[str, str]) -> pd.DataFrame:
    nodes: Dict[Tuple[str, str], Dict[str, Any]] = {}
    edge_count = defaultdict(int)

    for edge in edges:
        if not isinstance(edge, dict):
            continue

        for field, node_type in entity_fields.items():
            if field not in edge or is_blank(edge[field]):
                continue

            label, existing_curie = extract_label_and_curie(edge[field])
            if is_blank(label) or str(label).strip() == "Not reported":
                continue

            label = str(label).strip()
            key = (label, node_type)

            if key not in nodes:
                nodes[key] = {
                    "original_node_label": label,
                    "node_type": node_type,
                    "existing_curie": existing_curie or "",
                }
            else:
                if is_blank(nodes[key].get("existing_curie")) and not is_blank(existing_curie):
                    nodes[key]["existing_curie"] = str(existing_curie).strip()

            edge_count[key] += 1

    rows: List[Dict[str, Any]] = []
    for (label, node_type), rec in nodes.items():
        rec["n_edges"] = int(edge_count[(label, node_type)])
        rows.append(rec)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    return df.sort_values(["node_type", "n_edges", "original_node_label"], ascending=[True, False, True])


def looks_like_curie(s: str) -> bool:
    if is_blank(s):
        return False
    s = str(s).strip()
    # cheap CURIE-ish test (prefix:suffix), but avoid URLs
    return (":" in s) and (not s.lower().startswith("http")) and (not s.lower().startswith("urn:"))


# ---------------------------
# main
# ---------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve CUREID JSON nodes via Translator SRI Name Resolver (improved, no LLM cleaning)."
    )
    parser.add_argument("json_in", help="Input JSON file, e.g. cureid_cases_12.16.25.json")
    parser.add_argument("--outdir", default="data/output", help="Output directory (default: data/output)")
    parser.add_argument("--tag", default=None, help="Optional output tag override")

    # SRI controls
    parser.add_argument("--sri_max_hits", type=int, default=10, help="Max hits to request from SRI (default: 10)")
    parser.add_argument("--sri_min_score", type=float, default=0.0, help="Minimum SRI score threshold (default: 0.0)")
    parser.add_argument("--sri_delay_s", type=float, default=0.10, help="Delay between SRI calls (seconds)")
    parser.add_argument("--sri_cache_dir", default=".cache/sri_name_resolver", help="Disk cache directory")

    parser.add_argument("--log_level", default="INFO", help="Logging level (INFO, DEBUG, WARNING, ...)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("cureid_sri_resolve")

    json_path = Path(args.json_in)
    if not json_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {json_path}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    tag = args.tag or derive_tag_from_input(json_path)
    cache_dir = Path(args.sri_cache_dir) if args.sri_cache_dir else None

    logger.info("Loading JSON: %s", json_path)
    edges = load_edges(json_path)
    logger.info("Found %d edge records", len(edges))

    logger.info("Extracting unique nodes")
    nodes_df = build_nodes_table(edges, DEFAULT_ENTITY_FIELDS)
    if nodes_df.empty:
        logger.warning("No nodes extracted; nothing to write.")
        return

    logger.info("Extracted %d unique nodes", len(nodes_df))

    session = setup_session()
    out_rows: List[Dict[str, Any]] = []

    logger.info("Resolving nodes via SRI (SequenceVariant forced UNMAPPABLE; existing_curie premaps)")
    for idx, row in nodes_df.reset_index(drop=True).iterrows():
        label = row["original_node_label"]
        node_type = row["node_type"]
        existing_curie = row.get("existing_curie", "")

        rec = row.to_dict()
        rec["sri_query"] = label

        # Force SequenceVariant to UNMAPPABLE and do NOT query SRI
        if node_type == "SequenceVariant":
            rec.update(
                {
                    "sri_best_curie": "",
                    "sri_best_label": "",
                    "sri_best_score": "",
                    "sri_best_categories": "",
                    "sri_best_synonyms": "",
                    "sri_top_hits_json": "",
                    "sri_match_type": "UNMAPPABLE",
                    "resolution_source": "FORCED",
                }
            )
        # If CUREID already provided a CURIE, treat as premapped-resolved and skip SRI
        elif looks_like_curie(existing_curie):
            rec.update(
                {
                    "sri_best_curie": str(existing_curie).strip(),
                    "sri_best_label": label,     # treat as exact by definition for QC triage
                    "sri_best_score": 1.0,
                    "sri_best_categories": "",
                    "sri_best_synonyms": "",
                    "sri_top_hits_json": "",
                    "sri_match_type": "EXACT",
                    "resolution_source": "CUREID_PREMAPPED",
                }
            )
        else:
            biolink_types = NODETYPE_TO_BIOLINK_TYPES.get(node_type, [])

            # Query prep:
            # - Disease/Gene/Drug => single cleaned query
            # - Phenotype/AE => chunk into up to 3 candidates
            if node_type in {"PhenotypicFeature", "AdverseEvent"}:
                queries = chunk_feature_string(label)
            else:
                queries = [clean_name_for_sri(label)]

            hits: List[Dict[str, Any]] = []
            best: Optional[Dict[str, Any]] = None
            used_query = ""

            for q in queries:
                if is_blank(q):
                    continue
                used_query = q
                hits = sri_lookup(
                    session=session,
                    query=q,
                    biolink_types=biolink_types,
                    max_hits=args.sri_max_hits,
                    min_score=args.sri_min_score,
                    delay_s=args.sri_delay_s,
                    logger=logger,
                    cache_dir=cache_dir,
                )
                best = pick_best_hit(hits)
                if best is not None:
                    break

            rec["sri_query"] = used_query or label

            if best is None:
                rec.update(
                    {
                        "sri_best_curie": "",
                        "sri_best_label": "",
                        "sri_best_score": "",
                        "sri_best_categories": "",
                        "sri_best_synonyms": "",
                        "sri_top_hits_json": "",
                        "sri_match_type": "UNRESOLVED",
                        "resolution_source": "SRI",
                    }
                )
            else:
                rec["sri_best_curie"] = best.get("curie", "") or best.get("id", "") or best.get("identifier", "") or ""
                rec["sri_best_label"] = best.get("label", "") or best.get("name", "") or ""
                rec["sri_best_score"] = best.get("score", "") or best.get("confidence", "") or ""

                cats = best.get("categories") or best.get("category") or []
                syns = best.get("synonyms") or []
                rec["sri_best_categories"] = "|".join(map(str, cats)) if isinstance(cats, list) else str(cats)
                rec["sri_best_synonyms"] = "|".join(map(str, syns)) if isinstance(syns, list) else str(syns)
                rec["sri_top_hits_json"] = safe_json_dumps(hits)

                rec["sri_match_type"] = match_type(label, rec["sri_best_label"], syns)
                rec["resolution_source"] = "SRI"

        rec["sri_any_non_exact"] = rec["sri_match_type"] in {"NON_EXACT", "UNRESOLVED", "UNMAPPABLE"}
        rec["sri_any_unresolved"] = rec["sri_match_type"] in {"UNRESOLVED", "UNMAPPABLE"}

        if (idx + 1) % 50 == 0:
            logger.info("Resolved %d / %d", idx + 1, len(nodes_df))

        out_rows.append(rec)

    full_df = pd.DataFrame(out_rows)

    # Review file = anything not EXACT (includes UNMAPPABLE)
    review_df = full_df[full_df["sri_match_type"] != "EXACT"].copy()

    semi_dir = outdir / "semi"
    qc_dir = outdir / "qc"

    semi_dir.mkdir(parents=True, exist_ok=True)
    qc_dir.mkdir(parents=True, exist_ok=True)

    full_out = semi_dir / f"cureid_resolved_full_{tag}.xlsx"
    review_out = qc_dir / f"SRI_nodes_non_exact_for_review_{tag}.xlsx"

    logger.info("Writing full output: %s", full_out)
    full_df.to_excel(full_out, index=False)

    logger.info("Writing review output: %s", review_out)
    review_df.to_excel(review_out, index=False)

    logger.info("Done. Full rows: %d; review rows: %d", len(full_df), len(review_df))


if __name__ == "__main__":
    main()