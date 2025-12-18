#!/usr/bin/env python3
"""
cureid_apply_final_ids.py — Build CureID edges from JSON and apply final curated CURIEs/labels

Inputs:
  1) CureID JSON (new or old format)
  2) Final node mapping table (your merged+QC file) containing:
       original_node_label
       node_type
       final_curie_id
       final_curie_label

Behavior:
  - Builds edges from JSON (drug->disease, drug->phenotype targets, drug->AE, disease->phenotype, gene->disease,
    plus synthesized gene->variant and variant->disease)
  - Ignores CURIEs embedded in the JSON (does NOT trust disease_curie_id or any other inputs)
  - Overwrites subject/object curie+label using the mapping table
  - Emits TSV with standardized IDs/labels and QC flags for missing mappings

Example:
  python cureid_apply_final_ids.py \
    --json_in data/input/cureid_cases_12.16.25.json \
    --final_nodes_xlsx cureid_resolved_full_with_manualQC_final_curie.xlsx \
    --out_tsv data/output/cureid_edges_final_12.16.25.tsv
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# CureID → Biolink mapping (kept from your prior script structure)
RELATION_MAP = {
    "applied_to_treat": {
        "biolink_predicate": "biolink:applied_to_treat",
        "association_category": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
    },
    "has_adverse_events": {
        "biolink_predicate": "biolink:has_adverse_event",
        "association_category": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
    },
    "has_phenotype_of": {
        "biolink_predicate": "biolink:has_phenotype",
        "association_category": "biolink:DiseaseToPhenotypicFeatureAssociation",
    },
    "gene_associated_with_condition": {
        "biolink_predicate": "biolink:gene_associated_with_condition",
        "association_category": "biolink:GeneToDiseaseAssociation",
    },
    # synthesized edges
    "has_sequence_variant": {
        "biolink_predicate": "biolink:has_sequence_variant",
        "association_category": "biolink:GeneToVariantAssociation",
    },
    "genetically_associated_with": {
        "biolink_predicate": "biolink:genetically_associated_with",
        "association_category": "biolink:VariantToDiseaseAssociation",
    },
}

PAREN_RE = re.compile(r"\s*\([^)]*\)")

def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("cureid_apply_final_ids")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger

def clean_text(x: Any) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x).strip())

def split_targets(s: str) -> List[str]:
    s = clean_text(s)
    if not s:
        return []
    if ";" in s:
        parts = [p.strip(" ;,") for p in s.split(";")]
    else:
        if s.count(" and ") >= 2 and "," not in s:
            parts = [p.strip(" ;,") for p in s.split(" and ")]
        else:
            parts = [s]
    out, seen = [], set()
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out

def load_final_node_map(path: Path, logger: logging.Logger) -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Returns dict keyed by (original_node_label, node_type) -> {"final_curie_id":..., "final_curie_label":...}
    """
    logger.info(f"📥 Loading final node map: {path}")
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        # allow TSV/CSV too
        sep = "\t" if path.suffix.lower() in (".tsv", ".txt") else ","
        df = pd.read_csv(path, sep=sep, dtype=str)

    needed = {"original_node_label", "node_type", "final_curie_id", "final_curie_label"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"final_nodes file missing columns: {sorted(missing)}")

    df = df.fillna("")
    m: Dict[Tuple[str, str], Dict[str, str]] = {}
    for _, r in df.iterrows():
        key = (clean_text(r["original_node_label"]), clean_text(r["node_type"]))
        if not key[0] or not key[1]:
            continue
        m[key] = {
            "final_curie_id": clean_text(r["final_curie_id"]),
            "final_curie_label": clean_text(r["final_curie_label"]),
        }

    logger.info(f"   Final node mappings loaded: {len(m):,}")
    return m

def read_cureid_json(json_path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(json_path.read_text())
    return raw["data"] if isinstance(raw, dict) and "data" in raw else raw

def build_edges_from_json(json_path: Path, logger: logging.Logger) -> pd.DataFrame:
    """
    IMPORTANT: This function intentionally ignores any CURIE IDs in the JSON.
    It only emits labels/types + relationship/context.
    """
    t0 = time.perf_counter()
    logger.info(f"📥 Reading CureID JSON: {json_path}")
    data = read_cureid_json(json_path)
    logger.info(f"   Loaded {len(data):,} rows from JSON.")

    # Cache gene & variant context per report
    report_gene: Dict[str, str] = {}
    report_var: Dict[str, str] = {}

    for rec in data:
        rid = clean_text(rec.get("report_id"))
        rel = clean_text(rec.get("relationship"))
        if not rid or rel != "condition_associated_with_gene":
            continue
        gene = clean_text(rec.get("gene"))
        nuc  = clean_text(rec.get("nucleotide_change"))
        prot = clean_text(rec.get("protein_change"))
        if gene:
            report_gene[rid] = gene
        if nuc or prot:
            parts = [p for p in [gene, nuc, prot] if p]
            if parts:
                report_var[rid] = " ".join(parts)

    edges: List[Dict[str, Any]] = []

    def add_edge(sub_label, sub_type, pred_raw, obj_label, obj_type, **attrs):
        if not (sub_label and obj_label and pred_raw):
            return
        sub_label_clean = clean_text(sub_label)
        obj_label_clean = clean_text(obj_label)

        biolink_pred = RELATION_MAP.get(pred_raw, {}).get("biolink_predicate", "")
        assoc_cat    = RELATION_MAP.get(pred_raw, {}).get("association_category", "")

        edges.append({
            "subject_label_original": sub_label,
            "object_label_original":  obj_label,

            # labels used for lookup into final mapping table
            "subject_label": sub_label_clean,
            "subject_type":  sub_type,
            "predicate_raw": pred_raw,
            "biolink_predicate": biolink_pred,
            "association_category": assoc_cat,
            "object_label": obj_label_clean,
            "object_type":  obj_type,

            # JSON provenance fields (kept)
            **{k: clean_text(v) for k, v in attrs.items()}
        })

    for rec in data:
        rid  = clean_text(rec.get("report_id", ""))
        rel  = clean_text(rec.get("relationship"))
        pmid = clean_text(rec.get("pmid", ""))
        link = clean_text(rec.get("link", ""))
        outc = clean_text(rec.get("outcome", ""))

        if rel == "applied_to_treat":
            drug = clean_text(rec.get("drug", ""))
            dis  = clean_text(rec.get("disease", ""))
            pt   = clean_text(rec.get("primary_target", ""))
            st   = clean_text(rec.get("secondary_target", ""))

            if drug and dis:
                add_edge(drug, "Drug", rel, dis, "Disease",
                         report_id=rid, pmid=pmid, link=link, outcome=outc)

            # keep these as PhenotypicFeature per your earlier script behavior
            for feat in [*split_targets(pt), *split_targets(st)]:
                add_edge(drug, "Drug", rel, feat, "PhenotypicFeature",
                         report_id=rid, pmid=pmid, link=link, outcome=outc)

        elif rel == "has_adverse_events":
            drug = clean_text(rec.get("drug", ""))
            ae   = clean_text(rec.get("adverse_event", ""))
            if drug and ae:
                add_edge(drug, "Drug", rel, ae, "AdverseEvent",
                         report_id=rid, pmid=pmid, link=link, outcome=outc)

        elif rel == "has_phenotype_of":
            dis  = clean_text(rec.get("disease", ""))
            phe  = clean_text(rec.get("phenotype", ""))
            if dis and phe:
                add_edge(dis, "Disease", rel, phe, "PhenotypicFeature",
                         report_id=rid, pmid=pmid, link=link, outcome=outc)

        elif rel == "condition_associated_with_gene":
            dis  = clean_text(rec.get("disease", ""))
            gene = clean_text(rec.get("gene", "")) or report_gene.get(rid, "")
            if gene and dis:
                add_edge(gene, "Gene", "gene_associated_with_condition", dis, "Disease",
                         report_id=rid, pmid=pmid, link=link, outcome=outc)

    # Synthesize gene→variant and variant→disease
    for rec in data:
        rid = clean_text(rec.get("report_id", ""))
        if not rid:
            continue
        dis  = clean_text(rec.get("disease", ""))
        gene = report_gene.get(rid, "")
        var  = report_var.get(rid, "")
        if gene and var and dis:
            add_edge(gene, "Gene", "has_sequence_variant", var, "SequenceVariant",
                     report_id=rid, pmid=clean_text(rec.get("pmid", "")), link=clean_text(rec.get("link", "")))
            add_edge(var, "SequenceVariant", "genetically_associated_with", dis, "Disease",
                     report_id=rid, pmid=clean_text(rec.get("pmid", "")), link=clean_text(rec.get("link", "")))

    df = pd.DataFrame(edges).drop_duplicates()
    logger.info(f"🧱 Built edges from JSON: {len(df):,} rows (took {time.perf_counter()-t0:.2f}s).")
    return df

def apply_final_ids(edges: pd.DataFrame,
                    node_map: Dict[Tuple[str, str], Dict[str, str]],
                    logger: logging.Logger) -> pd.DataFrame:
    """
    Overwrite subject/object curie+label using the final mapping table.
    """
    for col in [
        "subject_final_curie", "subject_final_label",
        "object_final_curie",  "object_final_label",
        "subject_missing_final", "object_missing_final",
    ]:
        if col not in edges.columns:
            edges[col] = ""

    def lookup(label: str, typ: str) -> Tuple[str, str, bool]:
        key = (clean_text(label), clean_text(typ))
        rec = node_map.get(key)
        if not rec:
            return "", "", True
        cur = clean_text(rec.get("final_curie_id", ""))
        lab = clean_text(rec.get("final_curie_label", ""))
        missing = (not cur) or (not lab)
        return cur, lab, missing

    subj_missing = 0
    obj_missing = 0

    for i, r in edges.iterrows():
        sc, sl, sm = lookup(r["subject_label"], r["subject_type"])
        oc, ol, om = lookup(r["object_label"], r["object_type"])

        edges.at[i, "subject_final_curie"] = sc
        edges.at[i, "subject_final_label"] = sl
        edges.at[i, "subject_missing_final"] = "Y" if sm else "N"

        edges.at[i, "object_final_curie"] = oc
        edges.at[i, "object_final_label"] = ol
        edges.at[i, "object_missing_final"] = "Y" if om else "N"

        subj_missing += 1 if sm else 0
        obj_missing  += 1 if om else 0

    logger.info(f"🔧 Applied final IDs/labels.")
    logger.info(f"   Subject missing final mapping (or blank final fields): {subj_missing:,}/{len(edges):,}")
    logger.info(f"   Object  missing final mapping (or blank final fields): {obj_missing:,}/{len(edges):,}")

    return edges

def explode_paired(df: pd.DataFrame, id_col: str, label_col: str, sep: str = "|", qc_col: str | None = None) -> pd.DataFrame:
    """
    Explode paired pipe-delimited columns (id_col + label_col) into aligned rows.

    - If both are empty -> keep 1 row with ("","") so you don't lose edges.
    - If counts mismatch -> zip to min length and flag qc_col = Y.
    """
    if qc_col is None:
        qc_col = f"qc_pair_count_mismatch__{id_col}"

    def split_list(x):
        if pd.isna(x):
            return []
        s = str(x).strip()
        if not s:
            return []
        return [p.strip() for p in s.split(sep)]

    pairs_col: list[list[tuple[str, str]]] = []
    mismatch_col: list[str] = []

    ids_series = df[id_col].apply(split_list)
    labels_series = df[label_col].apply(split_list)

    for ids, labels in zip(ids_series.tolist(), labels_series.tolist()):
        if not ids and not labels:
            pairs_col.append([("", "")])          # keep one row
            mismatch_col.append("N")
            continue

        mismatch_col.append("Y" if len(ids) != len(labels) else "N")
        n = min(len(ids), len(labels))
        if n == 0:
            pairs_col.append([("", "")])          # keep one row even if one side missing
        else:
            pairs_col.append(list(zip(ids[:n], labels[:n])))

    out = df.copy()
    out["_pair"] = pairs_col
    out[qc_col] = mismatch_col

    out = out.explode("_pair", ignore_index=True)
    out[id_col] = out["_pair"].apply(lambda t: t[0])
    out[label_col] = out["_pair"].apply(lambda t: t[1])
    out = out.drop(columns=["_pair"])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json_in", required=True, help="CureID JSON input")
    ap.add_argument("--final_nodes_xlsx", required=True,
                    help="Your final node table (xlsx/tsv/csv) with original_node_label,node_type,final_curie_id,final_curie_label")
    ap.add_argument("--out_tsv", required=True, help="Output edges TSV")
    ap.add_argument("--log_level", default="INFO")
    args = ap.parse_args()

    logger = setup_logger(args.log_level)

    json_in = Path(args.json_in)
    final_nodes = Path(args.final_nodes_xlsx)
    out_tsv = Path(args.out_tsv)
    out_tsv.parent.mkdir(parents=True, exist_ok=True)

    node_map = load_final_node_map(final_nodes, logger)
    edges = build_edges_from_json(json_in, logger)
    edges = apply_final_ids(edges, node_map, logger)
    # Explode multi-curie mappings (paired) into multiple edges
    edges = explode_paired(edges, "subject_final_curie", "subject_final_label")
    edges = explode_paired(edges, "object_final_curie", "object_final_label")


    # Output columns: keep the old “raw/original” labels + the corrected label+id
    out_cols = [
        "subject_label_original",
        "subject_label",
        "subject_type",
        "subject_final_label",
        "subject_final_curie",
        "subject_missing_final",

        "predicate_raw",
        "biolink_predicate",
        "association_category",

        "object_label_original",
        "object_label",
        "object_type",
        "object_final_label",
        "object_final_curie",
        "object_missing_final",

        "report_id",
        "pmid",
        "link",
        "outcome",
    ]
    for c in out_cols:
        if c not in edges.columns:
            edges[c] = ""

    edges[out_cols].to_csv(out_tsv, sep="\t", index=False)
    logger.info(f"💾 Wrote: {out_tsv} ({len(edges):,} edges)")

if __name__ == "__main__":
    main()
