#!/usr/bin/env python3
"""
cataract_metabo_resolver.py — Resolve cataracts metabolite names using Translator SRI

Usage:
    python cataract_metabo_resolver.py \
        --input cataracts_sig_metabolites.csv \
        --output cataracts_sig_metabolites_sri.tsv

Input:
    CSV with at least:
        - CHEMICAL_NAME

Output:
    TSV with:
        - CHEMICAL_NAME
        - sri_query
        - sri_best_curie
        - sri_best_label
        - sri_best_score
        - sri_best_synonyms   (pipe-delimited)
        - sri_best_categories (pipe-delimited)
        - sri_top5_hits_json  (JSON list of {curie,label,score,category,synonyms})
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# SRI settings (chem-focused)
SRI_BASES = [
    "https://name-resolution-sri.renci.org/lookup",
]
SRI_MIN_SCORE = 0.0
SRI_MAX_HITS = 10
SRI_DELAY_S = 0.05

SRI_TYPES_CHEM = [
    "biolink:Drug",
    "biolink:ChemicalSubstance",
    "biolink:SmallMolecule",
    "biolink:ChemicalEntity",
]


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("cataract_sri_resolver")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# --------------------------------------------------------------------------
# HTTP session + SRI helpers
# --------------------------------------------------------------------------
def build_requests_session(logger: logging.Logger) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"Accept": "application/json"})
    logger.debug("HTTP session with retries initialized.")
    return s


def sri_request(
    session: requests.Session,
    base: str,
    name: str,
    kinds: List[str],
    min_score: float,
    max_hits: int,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    params = [
        ("string", name),
        ("limit", str(max_hits)),
        ("offset", "0"),
        ("autocomplete", "false"),
        ("highlighting", "false"),
    ]
    for t in kinds:
        params.append(("biolink_type", t))

    r = session.get(base, params=params, timeout=30)
    r.raise_for_status()
    raw = r.json()
    results = raw.get("results", raw) if isinstance(raw, dict) else raw

    hits: List[Dict[str, Any]] = []
    for it in results or []:
        curie = it.get("curie") or it.get("id") or it.get("identifier")
        label = it.get("label") or it.get("name") or ""
        score = float(it.get("score") or it.get("confidence") or 0.0)
        cats = it.get("category") or it.get("categories") or []
        syns = it.get("synonyms") or []
        if curie and score >= min_score:
            hits.append(
                {
                    "curie": curie,
                    "label": label,
                    "score": score,
                    "category": cats,
                    "synonyms": syns,
                }
            )

    # de-dupe by curie and sort by score desc
    seen = set()
    out: List[Dict[str, Any]] = []
    for h in sorted(hits, key=lambda x: x["score"], reverse=True):
        if h["curie"] in seen:
            continue
        seen.add(h["curie"])
        out.append(h)
    return out[:max_hits]


def sri_lookup(name: str, session: requests.Session, logger: logging.Logger) -> List[Dict[str, Any]]:
    name = (name or "").strip()
    if not name:
        return []

    tries = [
        {"bases": SRI_BASES, "kinds_sets": [SRI_TYPES_CHEM]},
        {"bases": SRI_BASES, "kinds_sets": [[SRI_TYPES_CHEM[0]]]},  # just biolink:Drug
    ]

    for tier in tries:
        for base in tier["bases"]:
            for ks in tier["kinds_sets"]:
                try:
                    hits = sri_request(
                        session=session,
                        base=base,
                        name=name,
                        kinds=ks,
                        min_score=SRI_MIN_SCORE,
                        max_hits=SRI_MAX_HITS,
                        logger=logger,
                    )
                    if hits:
                        time.sleep(SRI_DELAY_S)
                        return hits
                except Exception as e:
                    logger.warning(f"SRI request failed for {name!r} @ {base} (types={ks}): {e}")
                    time.sleep(0.6)
    return []


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Resolve cataract metabolites via SRI Name Resolver.")
    ap.add_argument(
        "--input",
        default="cataract_sig_mets.csv",
        help="Input CSV with CHEMICAL_NAME column (default: cataract_sig_mets.csv)",
    )
    ap.add_argument(
        "--output",
        default="cataract_sig_mets_sri.tsv",
        help="Output CSV with SRI mappings (default: cataract_sig_mets_sri.tsv)",
    )
    ap.add_argument(
        "--log_level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    args = ap.parse_args()

    logger = setup_logger(args.log_level)
    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        sys.exit(1)

    logger.info(f"Reading input table: {in_path}")

    # Robust delimiter handling: .tsv/.txt => tab, otherwise try auto-detect (falls back to comma)
    suffix = in_path.suffix.lower()
    if suffix in [".tsv", ".tab", ".txt"]:
        df = pd.read_csv(in_path, sep="\t", dtype=str).fillna("")
    else:
        # sep=None + engine="python" triggers delimiter sniffing
        df = pd.read_csv(in_path, sep=None, engine="python", dtype=str).fillna("")

    logger.info(f"Detected columns: {list(df.columns)}")

    if "CHEMICAL_NAME" not in df.columns:
        logger.error("Input file must contain a 'CHEMICAL_NAME' column.")
        sys.exit(1)

    # Prepare unique queries to avoid redundant SRI calls
    all_names = df["CHEMICAL_NAME"].astype(str).str.strip()
    unique_names = sorted({n for n in all_names if n})

    logger.info(f"Unique CHEMICAL_NAME values to resolve: {len(unique_names):,}")

    sess = build_requests_session(logger)
    results_map: Dict[str, Dict[str, Any]] = {}

    for idx, name in enumerate(unique_names, start=1):
        hits = sri_lookup(name, sess, logger)
        if hits:
            best = hits[0]
            best_syns = best.get("synonyms") or []
            best_cats = best.get("category") or []

            results_map[name] = {
                "sri_query": name,
                "sri_best_curie": best.get("curie", ""),
                "sri_best_label": best.get("label", ""),
                "sri_best_score": float(best.get("score", 0.0)),
                "sri_best_synonyms": "|".join(str(s) for s in best_syns),
                "sri_best_categories": "|".join(str(c) for c in best_cats),
                "sri_top5_hits_json": json.dumps(hits, separators=(",", ":")),
            }
            logger.info(
                f"[{idx}/{len(unique_names)}] {name!r} → "
                f"{best.get('curie','')} ({best.get('label','')}) s={float(best.get('score',0.0)):.3f}"
            )
        else:
            results_map[name] = {
                "sri_query": name,
                "sri_best_curie": "",
                "sri_best_label": "",
                "sri_best_score": 0.0,
                "sri_best_synonyms": "",
                "sri_best_categories": "",
                "sri_top5_hits_json": "[]",
            }
            logger.info(f"[{idx}/{len(unique_names)}] {name!r} → NO HITS")

    # Attach back to original dataframe
    out_df = df.copy()
    out_df["sri_query"] = out_df["CHEMICAL_NAME"].astype(str).str.strip()

    # Initialize empty columns
    for col in [
        "sri_best_curie",
        "sri_best_label",
        "sri_best_score",
        "sri_best_synonyms",
        "sri_best_categories",
        "sri_top5_hits_json",
    ]:
        out_df[col] = ""

    # Fill from results_map
    for i, row in out_df.iterrows():
        name = row["sri_query"]
        res = results_map.get(name)
        if not res:
            continue
        out_df.at[i, "sri_best_curie"] = res["sri_best_curie"]
        out_df.at[i, "sri_best_label"] = res["sri_best_label"]
        out_df.at[i, "sri_best_score"] = res["sri_best_score"]
        out_df.at[i, "sri_best_synonyms"] = res["sri_best_synonyms"]
        out_df.at[i, "sri_best_categories"] = res["sri_best_categories"]
        out_df.at[i, "sri_top5_hits_json"] = res["sri_top5_hits_json"]

    logger.info(f"Writing output CSV: {out_path}")
    out_df.to_csv(out_path, sep="\t", index=False)
    logger.info("Done.")


if __name__ == "__main__":
    main()
