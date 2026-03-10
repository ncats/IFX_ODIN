# disease_merge.py - ODIN disease source harmonization pipeline (v2)
# Merges: Mondo, DOID, MedGen, Orphanet, OMIM, NodeNorm
# Produces: master CSV, harmonization Excel, focused human+LLM-readable QC files
#
# QC outputs (all in qc/ directory):
#   1. mismatches.csv        — one row per (disease, namespace) mismatch, showing ONLY
#                              the conflicting source values side-by-side
#   2. cardinality.csv       — actual ID pairs with 1:N or N:1 relationships
#   3. duplicate_mondo.csv   — mondo_ids appearing on multiple rows after merge
#   4. summary.csv           — per-namespace agreement/quality statistics
#   5. nodenorm_unmatched.csv — NodeNorm records that couldn't be matched

import os
import yaml
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from collections import Counter
import gc

# ─────────────────────────────────────────────
# Columns to drop (intermediate provenance junk from upstream cleaning)
# ─────────────────────────────────────────────
DROP_COLS = [
    "GARD_provenance_mondo", "ICDO_provenance_mondo", "EFO_provenance_mondo",
    "ICD9CM_doid_mondo", "ICD10CM_provenance_mondo", "MESH_provenance_mondo",
    "UMLS_provenance_mondo", "synonyms_provenance_mondo", "definition_provenance_mondo",
    "preferred_label_provenance_mondo", "MESH_doid_mondo", "ICD9CM_doid_mondo",
    "SNOMEDCT_US_2020_03_01_doid_mondo", "SNOMEDCT_US_2020_09_01_doid_mondo",
    "SNOMEDCT_US_2021_07_31_doid_mondo", "SNOMEDCT_US_2021_09_01_doid",
    "SNOMEDCT_US_2022_03_01_doid", "SNOMEDCT_US_2022_07_31_doid_mondo",
    "SNOMEDCT_US_2023_09_01_doid_mondo", "SNOMEDCT_US_2023_10_01_doid_mondo",
    "SNOMEDCT_US_2023_11_01_doid_mondo", "SNOMEDCT_US_2024_03_01_doid_mondo",
    "ICDO_doid_mondo", "preferred_label_doid_mondo", "definition_doid_mondo",
    "synonyms_doid_mondo", "ORPHANET_provenance_mondo", "database_cross_reference_doid_mondo",
    "EFO_doid_mondo", "GARD_doid_mondo", "ICD10CM_doid_mondo", "UMLS_CUI_mondo",
]

# ─────────────────────────────────────────────
# Per-identifier group definitions
# Maps: (canonical_namespace → list of (source_tag, column_name))
# source_tag identifies the database; column_name is the actual df column.
# First entry in each group is the "anchor" (primary key side).
# ─────────────────────────────────────────────
ID_GROUPS = [
    ("mondo",    [("mondo",    "mondo_id"),
                  ("medgen",   "medgen_MONDO"),
                  ("nodenorm", "nodenorm_MONDO"),
                  ("orphanet", "orphanet_MONDO"),
                  ("gard",     "gard_MONDO")]),
    ("DOID",     [("doid",     "doid_DOID"),
                  ("mondo",    "mondo_doid"),
                  ("nodenorm", "nodenorm_DOID")]),
    ("EFO",      [("mondo",    "mondo_efo"),
                  ("doid",     "doid_EFO"),
                  ("nodenorm", "nodenorm_EFO")]),
    ("GARD",     [("gard",     "gard_GARD"),
                  ("mondo",    "mondo_gard"),
                  ("doid",     "doid_GARD"),
                  ("medgen",   "medgen_GARD")]),
    ("HP",       [("mondo",    "mondo_hp"),
                  ("medgen",   "medgen_HPO"),
                  ("nodenorm", "nodenorm_HP")]),
    ("ICD10",    [("mondo",    "mondo_icd10cm"),
                  ("doid",     "doid_ICD10CM"),
                  ("orphanet", "orphanet_ICD-10"),
                  ("nodenorm", "nodenorm_ICD10")]),
    ("ICD9",     [("mondo",    "mondo_icd9"),
                  ("doid",     "doid_ICD9CM"),
                  ("nodenorm", "nodenorm_ICD9")]),
    ("ICD11",    [("doid",     "doid_ICD11"),
                  ("orphanet", "orphanet_ICD-11"),
                  ("nodenorm", "nodenorm_icd11")]),
    ("ICD11f",   [("mondo",    "mondo_icd11.foundation"),
                  ("nodenorm", "nodenorm_icd11.foundation")]),
    ("ICDO",     [("mondo",    "mondo_icdo"),
                  ("doid",     "doid_ICDO")]),
    ("KEGG",     [("doid",     "doid_KEGG"),
                  ("nodenorm", "nodenorm_KEGG")]),
    ("MESH",     [("mondo",    "mondo_mesh"),
                  ("doid",     "doid_MESH"),
                  ("medgen",   "medgen_MeSH"),
                  ("orphanet", "orphanet_MeSH"),
                  ("nodenorm", "nodenorm_MESH")]),
    ("MEDDRA",   [("mondo",    "mondo_meddra"),
                  ("doid",     "doid_MEDDRA"),
                  ("orphanet", "orphanet_MedDRA"),
                  ("nodenorm", "nodenorm_MEDDRA")]),
    ("NCIT",     [("mondo",    "mondo_ncit"),
                  ("doid",     "doid_NCI"),
                  ("nodenorm", "nodenorm_NCIT")]),
    ("OMIM",     [("omim",     "omim_OMIM"),
                  ("mondo",    "mondo_omim"),
                  ("doid",     "doid_MIM"),
                  ("medgen",   "medgen_OMIM"),
                  ("orphanet", "orphanet_OMIM"),
                  ("nodenorm", "nodenorm_OMIM")]),
    ("OMIMPS",   [("mondo",    "mondo_omimps"),
                  ("medgen",   "medgen_OMIM Phenotypic Series"),
                  ("nodenorm", "nodenorm_OMIMPS")]),
    ("SNOMEDCT", [("doid",     "doid_SNOMEDCT"),
                  ("medgen",   "medgen_SNOMEDCT_US"),
                  ("nodenorm", "nodenorm_SNOMEDCT")]),
    ("UMLS",     [("mondo",    "mondo_umls"),
                  ("doid",     "doid_UMLS_CUI"),
                  ("medgen",   "medgen_UMLS"),
                  ("orphanet", "orphanet_UMLS"),
                  ("nodenorm", "nodenorm_UMLS"),
                  ("gard",     "gard_UMLS")]),
    ("orphanet", [("orphanet", "orphanet_Orphanet_ID"),
                  ("mondo",    "mondo_orphanet"),
                  ("medgen",   "medgen_Orphanet"),
                  ("nodenorm", "nodenorm_orphanet")]),
    ("medgen",   [("medgen",   "medgen_MedGen"),
                  ("mondo",    "mondo_medgen"),
                  ("nodenorm", "nodenorm_medgen"),
                  ("gard",     "gard_MEDGEN")]),
]

# Label columns to carry through for human-readable QC context
LABEL_COLS = [
    "mondo_id", "mondo_preferred_label", "doid_preferred_label",
    "medgen_Preferred_Name", "orphanet_Disease_Name",
    "omim_preferred_label", "gard_name", "nodenorm_Nodenorm_name",
]

# Only these namespaces have obsolete-aware logic
OBSOLETE_AWARE_NAMESPACES = {"mondo", "DOID", "OMIM", "orphanet"}


# ─────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────

def setup_logging(log_file):
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(fmt)
            root.addHandler(fh)


def clean_df(df):
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df.replace(r'^\s*$', np.nan, regex=True)


def split_pipe(val) -> set:
    """Split a pipe-delimited value into a set of cleaned strings."""
    if pd.isna(val) or not str(val).strip():
        return set()
    return {v.strip() for v in str(val).split("|") if v.strip()}


def concat_unique(series):
    """Aggregate a series by taking the union of all pipe-delimited values."""
    bits = set()
    for val in series.dropna().astype(str):
        bits.update(x.strip() for x in val.split("|") if x.strip())
    return "|".join(sorted(bits)) if bits else pd.NA


def load_id_set(path, id_col=None):
    """Load a set of IDs from a CSV/TSV file (for obsolete ID lists)."""
    import re
    if not path or not os.path.exists(path):
        logging.warning(f"Missing obsolete file: {path}")
        return set()
    if str(path).endswith(".tsv"):
        df = pd.read_csv(path, sep="\t", dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    df = clean_df(df)
    if df.empty:
        return set()
    if id_col and id_col in df.columns:
        col = id_col
    else:
        candidates = [c for c in df.columns if c.lower().endswith("_id") or c.lower() == "id"]
        if not candidates:
            curie_pat = re.compile(r"^[A-Za-z]+:\d+$")
            for c in df.columns:
                sample = df[c].dropna().head(10).astype(str)
                if sample.apply(lambda x: bool(curie_pat.match(x))).mean() > 0.5:
                    candidates.append(c)
                    break
        col = candidates[0] if candidates else df.columns[0]
    ids = set(df[col].dropna().astype(str).str.strip())
    logging.info(f"  → Using column '{col}' from {Path(path).name}")
    return {x for x in ids if x}


# ─────────────────────────────────────────────
# Agreement & scoring (single implementation)
# ─────────────────────────────────────────────

def compute_agreement(src_vals: dict[str, set]) -> str:
    """
    Given {source_tag: set_of_ids}, return agreement label.
    - 'no_data'    : no sources have values
    - 'single'     : only one source has values
    - 'agree'      : all sources report identical ID sets
    - 'subset'     : all values overlap (one source is a subset/superset of another)
                     — this is a cardinality issue, NOT a true mismatch
    - 'majority'   : >50% of sources share the most common value but some disagree
    - 'conflict'   : sources report non-overlapping IDs (true mismatch)
    """
    present = {k: v for k, v in src_vals.items() if v}
    n = len(present)
    if n == 0:
        return "no_data"
    if n == 1:
        return "single"
    sets = list(present.values())
    if all(s == sets[0] for s in sets):
        return "agree"

    # Check if all values are subsets/supersets of each other (one-to-many, not conflict)
    # i.e. the union == the largest set, meaning no source has a value outside the others
    union_all = set.union(*sets)
    largest = max(sets, key=len)
    if union_all == largest:
        # Every source's values are contained within the largest source's values
        return "subset"

    # Check if there's ANY overlap at all between sources
    intersection_all = set.intersection(*sets)
    if intersection_all:
        # Some shared values but also some unique ones — partial overlap
        # Check majority: does any single value appear in >50% of sources?
        flat = [v for s in sets for v in s]
        most_common_count = Counter(flat).most_common(1)[0][1]
        if most_common_count > n / 2:
            return "majority"
        return "conflict"

    # No overlap at all — true conflict
    # But first check if majority still applies (value-level, not set-level)
    flat = [v for s in sets for v in s]
    most_common_count = Counter(flat).most_common(1)[0][1]
    if most_common_count > n / 2:
        return "majority"
    return "conflict"


def compute_quality(agreement: str, n_sources: int) -> int:
    """Integer quality score."""
    scores = {
        "no_data": 0,
        "conflict": -1,
        "single": 1,
        "majority": 2,
        "subset": 2,   # subset is a cardinality issue, not a quality problem
    }
    if agreement in scores:
        return scores[agreement]
    return min(n_sources, 4)  # agree


def compute_cardinality(anchor_vals: set, src_vals: set) -> str:
    """Row-level cardinality between anchor and union of source values."""
    if not anchor_vals or not src_vals:
        return "no_data"
    na, ns = len(anchor_vals), len(src_vals)
    if na == 1 and ns == 1:
        return "one_to_one"
    if na == 1 and ns > 1:
        return "one_to_many"
    if na > 1 and ns == 1:
        return "many_to_one"
    return "complex"


# ─────────────────────────────────────────────
# QC file builders — focused, human-readable
# ─────────────────────────────────────────────

def build_mismatch_qc(harm_df, id_groups, label_cols):
    """
    TRUE MISMATCHES ONLY: rows where sources report non-overlapping IDs.
    Filters to 'conflict' and 'majority' agreement — NOT 'subset' (which is
    a cardinality issue, not a real disagreement).

    One row per (disease, namespace). Shows disease identifiers, namespace,
    each source's value side by side, consensus union.
    """
    label_cols_present = [c for c in label_cols if c in harm_df.columns]
    records = []

    for canonical, pairs in id_groups:
        agr_col = f"{canonical}_agreement"
        if agr_col not in harm_df.columns:
            continue

        # Only true disagreements — NOT subset (one-to-many without conflict)
        mask = harm_df[agr_col].isin(["conflict", "majority"])

        # Exclude rows where obsolete removal resolves the flag
        obs_flag_col = f"{canonical}_obsolete_flag"
        if obs_flag_col in harm_df.columns:
            mask = mask & (harm_df[obs_flag_col] != "resolved")

        if not mask.any():
            continue

        sub = harm_df[mask]
        logging.info(f"    {canonical}: {mask.sum():,} mismatches")

        for _, row in sub.iterrows():
            rec = {}
            for lc in label_cols_present:
                rec[lc] = row.get(lc)
            rec["namespace"] = canonical
            rec["agreement"] = row[agr_col]

            # Source values side-by-side
            for src_tag, src_col in pairs:
                col_name = f"{src_tag}_{canonical}"
                if col_name in harm_df.columns:
                    val = row.get(col_name)
                    rec[f"{src_tag}_value"] = val if pd.notna(val) else ""

            val_col = f"{canonical}_value"
            rec["consensus_union"] = row.get(val_col, "")
            records.append(rec)

    return pd.DataFrame(records)


def build_cardinality_qc(harm_df, id_groups, label_cols):
    """
    CARDINALITY ISSUES: rows where sources have subset/superset relationships
    (one-to-many) OR non-1:1 cardinality between anchor and sources.
    Includes 'subset' agreement rows AND non-1:1 cardinality rows.

    One row per (disease, namespace). Shows disease identifiers, namespace,
    cardinality type, agreement type, source values side by side.
    """
    label_cols_present = [c for c in label_cols if c in harm_df.columns]
    records = []

    for canonical, pairs in id_groups:
        agr_col = f"{canonical}_agreement"
        card_col = f"{canonical}_cardinality"

        if agr_col not in harm_df.columns and card_col not in harm_df.columns:
            continue

        # Include: subset agreement OR non-1:1 cardinality
        mask = pd.Series(False, index=harm_df.index)
        if agr_col in harm_df.columns:
            mask |= harm_df[agr_col].eq("subset")
        if card_col in harm_df.columns:
            mask |= harm_df[card_col].isin(["one_to_many", "many_to_one", "complex"])

        # Exclude rows where obsolete removal resolves the flag
        obs_flag_col = f"{canonical}_obsolete_flag"
        if obs_flag_col in harm_df.columns:
            mask = mask & (harm_df[obs_flag_col] != "resolved")

        if not mask.any():
            continue

        sub = harm_df[mask]
        logging.info(f"    {canonical}: {mask.sum():,} cardinality issues")

        anchor_tag = pairs[0][0]

        for _, row in sub.iterrows():
            rec = {}
            for lc in label_cols_present:
                rec[lc] = row.get(lc)
            rec["namespace"] = canonical
            rec["agreement"] = row.get(agr_col, "") if agr_col in harm_df.columns else ""
            rec["cardinality"] = row.get(card_col, "") if card_col in harm_df.columns else ""

            # All source values side-by-side
            for src_tag, src_col in pairs:
                col_name = f"{src_tag}_{canonical}"
                if col_name in harm_df.columns:
                    val = row.get(col_name)
                    rec[f"{src_tag}_value"] = val if pd.notna(val) else ""

            # Count IDs for quick scanning
            anchor_col_name = f"{anchor_tag}_{canonical}"
            anchor_val = row.get(anchor_col_name, "") if anchor_col_name in harm_df.columns else ""
            anchor_set = split_pipe(anchor_val)
            src_union = set()
            for src_tag, _ in pairs[1:]:
                col_name = f"{src_tag}_{canonical}"
                if col_name in harm_df.columns:
                    val = row.get(col_name)
                    if pd.notna(val):
                        src_union.update(split_pipe(val))

            rec["n_anchor_ids"] = len(anchor_set)
            rec["n_source_ids"] = len(src_union)
            records.append(rec)

    return pd.DataFrame(records)


def build_duplicate_mondo_qc(df):
    """Flag mondo_ids that appear on multiple rows (merge artifact)."""
    if "mondo_id" not in df.columns:
        return pd.DataFrame()
    counts = df["mondo_id"].value_counts()
    dupes = counts[counts > 1].index
    if len(dupes) == 0:
        return pd.DataFrame()

    sub = df[df["mondo_id"].isin(dupes)].copy()
    # Keep only useful context columns
    keep = [c for c in LABEL_COLS if c in sub.columns]
    keep += [c for c in sub.columns if c.startswith("nodenorm_match")]
    # Add a few key xref columns to show why the dupe happened
    xref_cols = ["mondo_omim", "omim_OMIM", "mondo_orphanet",
                 "orphanet_Orphanet_ID", "medgen_UMLS"]
    keep += [c for c in xref_cols if c in sub.columns]
    keep = list(dict.fromkeys(keep))  # dedupe preserving order
    return sub[keep].sort_values("mondo_id")


def build_summary_qc(harm_df, id_groups):
    """Per-namespace agreement/quality statistics."""
    rows = []
    for canonical, _ in id_groups:
        agr_col = f"{canonical}_agreement"
        qlt_col = f"{canonical}_quality"
        if agr_col not in harm_df.columns:
            continue
        vc = harm_df[agr_col].value_counts(dropna=True).to_dict()
        n_no_data = vc.get("no_data", 0)
        n_total = int(harm_df[agr_col].notna().sum())
        # Average quality excluding no_data (score=0) rows
        if qlt_col in harm_df.columns:
            q_series = harm_df[qlt_col]
            q_nonzero = q_series[q_series != 0]
            avg = q_nonzero.mean() if len(q_nonzero) > 0 else np.nan
        else:
            avg = np.nan
        row_data = {
            "namespace": canonical,
            "n_with_data": n_total - n_no_data,
            "agree": vc.get("agree", 0),
            "subset": vc.get("subset", 0),
            "majority": vc.get("majority", 0),
            "conflict": vc.get("conflict", 0),
            "single": vc.get("single", 0),
            "no_data": n_no_data,
            "avg_quality": round(avg, 2) if pd.notna(avg) else "",
        }
        # Add obsolete flag counts if available
        obs_col = f"{canonical}_obsolete_flag"
        if obs_col in harm_df.columns:
            obs_vc = harm_df[obs_col].value_counts(dropna=True).to_dict()
            row_data["obsolete_resolved"] = obs_vc.get("resolved", 0)
            row_data["obsolete_contributes"] = obs_vc.get("contributes", 0)
            row_data["obsolete_present"] = obs_vc.get("present", 0)
        rows.append(row_data)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Harmonization output builder (single pass)
# ─────────────────────────────────────────────

def build_harmonization_output(df, id_groups, obsolete_sets=None):
    """
    Build the wide harmonization table (one row per disease) — VECTORIZED.

    For each ID namespace:
      {source}_{namespace}      — raw value from each source
      {namespace}_value         — consensus/union value
      {namespace}_sources       — comma-sep source tags that contributed
      {namespace}_agreement     — agree | majority | conflict | single | no_data
      {namespace}_quality       — integer score
      {namespace}_cardinality   — one_to_one | one_to_many | many_to_one | complex | no_data

    If obsolete_sets provided, also:
      {namespace}_obsolete_ids     — pipe-delimited obsolete IDs found
      {namespace}_obsolete_sources — which sources had them
      {namespace}_obsolete_flag    — resolved | contributes | present | (empty)

    Plus overall_quality (mean of non-zero quality scores).
    """
    if obsolete_sets is None:
        obsolete_sets = {}
    n_rows = len(df)
    n_groups = len(id_groups)
    total_steps = n_groups + 1  # +1 for overall_quality
    logging.info(f"  Harmonizing {n_rows:,} rows across {n_groups} namespaces...")

    result = {}

    # Label columns
    for lc in LABEL_COLS:
        if lc in df.columns:
            result[lc] = df[lc].values

    quality_matrix = np.zeros((n_rows, n_groups), dtype=np.int8)

    for g_idx, (canonical, pairs) in enumerate(id_groups):
        if (g_idx + 1) % 5 == 0 or g_idx == 0:
            logging.info(f"    [{g_idx+1}/{n_groups}] Processing {canonical}...")

        # Collect raw arrays for each source
        src_tags = []
        src_arrays = []
        for src_tag, src_col in pairs:
            col_name = f"{src_tag}_{canonical}"
            if src_col in df.columns:
                arr = df[src_col].values
                result[col_name] = arr
                src_tags.append(src_tag)
                src_arrays.append(arr)
            else:
                result[col_name] = np.full(n_rows, np.nan, dtype=object)

        # Vectorized: compute per-row agreement, value, sources, quality, cardinality
        values_out = np.empty(n_rows, dtype=object)
        sources_out = np.empty(n_rows, dtype=object)
        agreement_out = np.empty(n_rows, dtype=object)
        quality_out = np.zeros(n_rows, dtype=np.int8)
        card_out = np.empty(n_rows, dtype=object)

        # Obsolete tracking arrays
        obs_set = obsolete_sets.get(canonical, set())
        obs_ids_out = np.empty(n_rows, dtype=object)
        obs_sources_out = np.empty(n_rows, dtype=object)
        obs_flag_out = np.empty(n_rows, dtype=object)
        obs_ids_out[:] = np.nan
        obs_sources_out[:] = np.nan
        obs_flag_out[:] = np.nan

        # Build boolean masks: which sources have data per row
        n_src = len(src_arrays)
        if n_src == 0:
            values_out[:] = np.nan
            sources_out[:] = np.nan
            agreement_out[:] = "no_data"
            quality_out[:] = 0
            card_out[:] = "no_data"
        else:
            # Parse all source values once (lists of sets)
            parsed = []
            for arr in src_arrays:
                parsed.append([split_pipe(v) for v in arr])

            # Anchor is first source's parsed values
            anchor_parsed = parsed[0] if parsed else [set()] * n_rows

            for i in range(n_rows):
                # Gather present sources
                present_tags = []
                present_sets = []
                for s_idx in range(n_src):
                    s = parsed[s_idx][i]
                    if s:
                        present_tags.append(src_tags[s_idx])
                        present_sets.append(s)

                n_present = len(present_tags)

                if n_present == 0:
                    values_out[i] = np.nan
                    sources_out[i] = np.nan
                    agreement_out[i] = "no_data"
                    quality_out[i] = 0
                else:
                    union = set()
                    for s in present_sets:
                        union.update(s)
                    values_out[i] = "|".join(sorted(union))
                    sources_out[i] = ",".join(present_tags)

                    if n_present == 1:
                        agr = "single"
                        quality_out[i] = 1
                    elif all(s == present_sets[0] for s in present_sets):
                        agr = "agree"
                        quality_out[i] = min(n_present, 4)
                    else:
                        flat = [v for s in present_sets for v in s]
                        mc = Counter(flat).most_common(1)[0][1]
                        if mc > n_present / 2:
                            agr = "majority"
                            quality_out[i] = 2
                        else:
                            agr = "conflict"
                            quality_out[i] = -1
                    agreement_out[i] = agr

                # Cardinality
                anchor_vals = anchor_parsed[i]
                src_union = set()
                for s_idx in range(1, n_src):
                    src_union.update(parsed[s_idx][i])

                if not anchor_vals or not src_union:
                    card_out[i] = "no_data"
                else:
                    na, ns = len(anchor_vals), len(src_union)
                    if na == 1 and ns == 1:
                        card_out[i] = "one_to_one"
                    elif na == 1 and ns > 1:
                        card_out[i] = "one_to_many"
                    elif na > 1 and ns == 1:
                        card_out[i] = "many_to_one"
                    else:
                        card_out[i] = "complex"

                # ── Obsolete ID checking ──
                if obs_set and n_present > 0:
                    row_obs_ids = set()
                    row_obs_sources = []
                    active_tags = []
                    active_sets = []

                    for s_idx in range(n_src):
                        raw = parsed[s_idx][i]
                        if not raw:
                            continue
                        obs_in_src = raw & obs_set
                        if obs_in_src:
                            row_obs_ids.update(obs_in_src)
                            row_obs_sources.append(src_tags[s_idx])
                        active = raw - obs_set
                        if active:
                            active_tags.append(src_tags[s_idx])
                            active_sets.append(active)

                    if row_obs_ids:
                        obs_ids_out[i] = "|".join(sorted(row_obs_ids))
                        obs_sources_out[i] = ",".join(sorted(row_obs_sources))

                        # Recompute agreement with obsoletes removed
                        active_agr = compute_agreement(
                            dict(zip(active_tags, active_sets)))

                        raw_agr = agreement_out[i]
                        raw_card = card_out[i]

                        # Recompute cardinality with obsoletes removed
                        active_anchor = (anchor_parsed[i] - obs_set) if anchor_parsed[i] else set()
                        active_src_union = set()
                        for s_idx in range(1, n_src):
                            active_src_union.update(parsed[s_idx][i] - obs_set)
                        active_card = compute_cardinality(active_anchor, active_src_union)

                        # Classify: does removing obsoletes resolve the flag?
                        mismatch_resolved = (
                            raw_agr in ("conflict", "majority") and
                            active_agr in ("agree", "single", "subset", "no_data"))
                        card_resolved = (
                            raw_card in ("one_to_many", "many_to_one", "complex") and
                            active_card in ("one_to_one", "no_data"))

                        if mismatch_resolved or card_resolved:
                            obs_flag_out[i] = "resolved"
                        else:
                            # Check if it at least improved
                            agr_rank = {"conflict": 0, "majority": 1, "subset": 2,
                                        "single": 3, "agree": 4, "no_data": -1}
                            if agr_rank.get(active_agr, -1) > agr_rank.get(raw_agr, -1):
                                obs_flag_out[i] = "contributes"
                            elif active_card != raw_card and raw_card != "no_data":
                                obs_flag_out[i] = "contributes"
                            else:
                                obs_flag_out[i] = "present"

        result[f"{canonical}_value"] = values_out
        result[f"{canonical}_sources"] = sources_out
        result[f"{canonical}_agreement"] = agreement_out
        result[f"{canonical}_quality"] = quality_out
        result[f"{canonical}_cardinality"] = card_out
        result[f"{canonical}_obsolete_ids"] = obs_ids_out
        result[f"{canonical}_obsolete_sources"] = obs_sources_out
        result[f"{canonical}_obsolete_flag"] = obs_flag_out
        quality_matrix[:, g_idx] = quality_out

    # Overall quality: mean of positive quality scores per row
    logging.info(f"    Computing overall quality scores...")
    pos_mask = quality_matrix > 0
    pos_sums = np.where(pos_mask, quality_matrix, 0).sum(axis=1)
    pos_counts = pos_mask.sum(axis=1)
    overall = np.where(pos_counts > 0, np.round(pos_sums / pos_counts, 2), 0.0)
    result["overall_quality"] = overall

    logging.info(f"  ✅ Harmonization complete")
    return pd.DataFrame(result)


# ─────────────────────────────────────────────
# Main merger class
# ─────────────────────────────────────────────

class DiseaseDataMerger:
    def __init__(self, config):
        self.config = config["disease_merge"]
        self.qc_dir = self.config.get(
            "qc_dir", "src/data/publicdata/disease_data/qc")
        os.makedirs(self.qc_dir, exist_ok=True)
        self.obsolete_sets = self._load_obsolete_sets()

    def _load_obsolete_sets(self):
        """Load obsolete ID sets for namespaces that have them."""
        cfg = self.config
        file_keys = {
            "mondo":    ("mondo_obsolete_file",    "mondo_obsolete_id_col"),
            "DOID":     ("doid_obsolete_file",     "doid_obsolete_id_col"),
            "OMIM":     ("omim_obsolete_file",     "omim_obsolete_id_col"),
            "orphanet": ("orphanet_obsolete_file",  "orphanet_obsolete_id_col"),
        }
        sets_out = {}
        any_loaded = False
        for ns, (file_key, col_key) in file_keys.items():
            path = cfg.get(file_key)
            id_col = cfg.get(col_key)
            if path:
                sets_out[ns] = load_id_set(path, id_col)
                if sets_out[ns]:
                    any_loaded = True
                logging.info(f"Loaded {len(sets_out[ns]):,} obsolete IDs for {ns}")
            else:
                sets_out[ns] = set()
        if not any_loaded:
            logging.info("ℹ️  No obsolete ID files configured — skipping obsolete detection")
        return sets_out

    def load_and_clean(self, path):
        logging.info(f"  Loading {path}")
        return clean_df(pd.read_csv(path, dtype=str))

    def _qc_path(self, filename):
        return os.path.join(self.qc_dir, filename)

    # ── merge sources ─────────────────────────

    def merge_sources(self):
        cfg = self.config

        # MONDO + DOID
        logging.info("🧬 Merging MONDO + DOID...")
        df_mondo = self.load_and_clean(cfg['mondo_cleaned_file'])
        df_doid = self.load_and_clean(cfg['doid_cleaned_file'])
        df = pd.merge(df_mondo, df_doid, how="outer",
                       left_on="mondo_doid", right_on="doid_DOID")
        logging.info(f"  → {df.shape[0]:,} rows")
        del df_mondo, df_doid; gc.collect()

        # MedGen
        logging.info("🧬 Merging MedGen...")
        df_medgen = self.load_and_clean(cfg['medgen_cleaned_file'])
        mapped = pd.merge(
            df[df["mondo_id"].notna()],
            df_medgen[df_medgen["medgen_MONDO"].notna()],
            how="outer", left_on="mondo_id", right_on="medgen_MONDO"
        )
        unmatched = df_medgen[df_medgen["medgen_MONDO"].isna()]
        df = pd.concat([mapped, unmatched], ignore_index=True)
        logging.info(f"  → {df.shape[0]:,} rows")
        del df_medgen, mapped, unmatched; gc.collect()

        # Orphanet
        logging.info("🧬 Merging Orphanet...")
        df_orph = self.load_and_clean(cfg['orphanet_cleaned_file'])
        df = pd.merge(df, df_orph, how="outer",
                       left_on="mondo_orphanet", right_on="orphanet_Orphanet_ID")
        logging.info(f"  → {df.shape[0]:,} rows")
        del df_orph; gc.collect()

        # OMIM
        logging.info("🧬 Merging OMIM...")
        df_omim = self.load_and_clean(cfg['omim_cleaned_file'])
        df = pd.merge(df, df_omim, how="outer",
                       left_on="mondo_omim", right_on="omim_OMIM")
        logging.info(f"  → {df.shape[0]:,} rows")
        del df_omim; gc.collect()

        # GARD (RDIP curated rare disease list — Mondo-first, UMLS fallback)
        if cfg.get('gard_cleaned_file'):
            logging.info("🧬 Merging GARD (RDIP)...")
            df_gard = self.load_and_clean(cfg['gard_cleaned_file'])
            df = self._merge_gard(df, df_gard)
            del df_gard; gc.collect()

        # NodeNorm (MONDO → ORPHANET → UMLS precedence)
        logging.info("🧬 Merging NodeNorm (MONDO → ORPHANET → UMLS precedence)...")
        df_nn = self.load_and_clean(cfg['nodenorm_cleaned_file'])
        df = self._merge_nodenorm(df, df_nn)
        del df_nn; gc.collect()

        # Reconcile orphaned source rows caused by pipe-delimited merge keys
        # (e.g. mondo_omim="OMIM:102730|OMIM:301083" didn't exact-match omim_OMIM="OMIM:301083")
        df = self._reconcile_pipe_orphans(df)

        # Drop known junk columns
        to_drop = [c for c in DROP_COLS if c in df.columns]
        if to_drop:
            df.drop(columns=to_drop, inplace=True)
            logging.info(f"  Dropped {len(to_drop)} intermediate columns")

        return df

    def _merge_gard(self, df, df_gard):
        """
        Merge RDIP GARD rare disease list.
        Cascaded: Mondo ID first, then UMLS fallback for the ~223 without Mondo.
        Unmatched GARD records (new rare diseases not in other sources) are appended.
        """
        gard_cols = [c for c in df_gard.columns if c.startswith("gard_")]

        # Step 1: match on Mondo ID
        m1 = pd.merge(df, df_gard[df_gard["gard_MONDO"].notna()],
                       how="left", left_on="mondo_id", right_on="gard_MONDO")
        matched_mondo = m1[m1["gard_MONDO"].notna()].copy()
        matched_mondo["gard_match_source"] = "MONDO"
        remaining = m1[m1["gard_MONDO"].isna()].drop(
            columns=[c for c in gard_cols if c in m1.columns], errors="ignore")

        # Step 2: fallback match on UMLS for GARD records without Mondo
        gard_no_mondo = df_gard[df_gard["gard_MONDO"].isna() & df_gard["gard_UMLS"].notna()]
        if len(gard_no_mondo) > 0 and "medgen_UMLS" in remaining.columns:
            m2 = pd.merge(remaining, gard_no_mondo,
                           how="left", left_on="medgen_UMLS", right_on="gard_UMLS")
            matched_umls = m2[m2["gard_UMLS"].notna()].copy()
            matched_umls["gard_match_source"] = "UMLS"
            remaining = m2[m2["gard_UMLS"].isna()].drop(
                columns=[c for c in gard_cols if c in m2.columns], errors="ignore")
        else:
            matched_umls = pd.DataFrame()

        # Unmatched GARD records (new rare diseases)
        used_gard = set()
        for col in ["gard_GARD"]:
            for sub in [matched_mondo, matched_umls]:
                if col in sub.columns:
                    used_gard.update(sub[col].dropna().unique())
        unmatched_gard = df_gard[~df_gard["gard_GARD"].isin(used_gard)].copy()
        unmatched_gard["gard_match_source"] = "unmatched_gard"

        # Unmatched original rows
        merged_ids = pd.concat([
            matched_mondo["mondo_id"],
            matched_umls["mondo_id"] if len(matched_umls) > 0 else pd.Series(dtype=str)
        ], ignore_index=True).dropna().unique()
        unmatched_df = remaining[~remaining["mondo_id"].isin(merged_ids)].copy() \
            if "mondo_id" in remaining.columns else remaining.copy()

        # Combine
        parts = [matched_mondo, unmatched_df]
        if len(matched_umls) > 0:
            parts.insert(1, matched_umls)
        if len(unmatched_gard) > 0:
            parts.append(unmatched_gard)

        result = pd.concat(parts, ignore_index=True)
        result = result.loc[:, ~result.columns.str.endswith(("_x", "_y"))]

        n_matched = len(matched_mondo) + len(matched_umls)
        logging.info(f"  → {result.shape[0]:,} rows after GARD merge "
                     f"({n_matched:,} matched, {len(unmatched_gard):,} new GARD-only)")

        del m1, matched_mondo, matched_umls, unmatched_gard, unmatched_df
        gc.collect()
        return result

    def _merge_nodenorm(self, df, df_nn):
        """Cascaded NodeNorm merge: MONDO → ORPHANET → UMLS, then append unmatched."""
        nn_cols = [c for c in df_nn.columns if c.startswith("nodenorm_")]
        df_original = df.copy()

        # Step 1: match on MONDO
        m1 = pd.merge(df, df_nn[df_nn["nodenorm_MONDO"].notna()],
                       how="left", left_on="mondo_id", right_on="nodenorm_MONDO")
        matched_mondo = m1[m1["nodenorm_MONDO"].notna()].copy()
        matched_mondo["nodenorm_match_source"] = "MONDO"
        remaining = m1[m1["nodenorm_MONDO"].isna()].drop(
            columns=[c for c in nn_cols if c in m1.columns], errors="ignore")

        # Step 2: match on ORPHANET
        m2 = pd.merge(remaining, df_nn[df_nn["nodenorm_orphanet"].notna()],
                       how="left", left_on="orphanet_Orphanet_ID",
                       right_on="nodenorm_orphanet")
        matched_orph = m2[m2["nodenorm_orphanet"].notna()].copy()
        matched_orph["nodenorm_match_source"] = "ORPHANET"
        remaining = m2[m2["nodenorm_orphanet"].isna()].drop(
            columns=[c for c in nn_cols if c in m2.columns], errors="ignore")

        # Step 3: match on UMLS
        m3 = pd.merge(remaining, df_nn[df_nn["nodenorm_UMLS"].notna()],
                       how="left", left_on="medgen_UMLS", right_on="nodenorm_UMLS")
        matched_umls = m3[m3["nodenorm_UMLS"].notna()].copy()
        matched_umls["nodenorm_match_source"] = "UMLS"
        remaining = m3[m3["nodenorm_UMLS"].isna()].drop(
            columns=[c for c in nn_cols if c in m3.columns], errors="ignore")

        # Identify unmatched NodeNorm rows (save for QC, but do NOT merge them
        # — these are overwhelmingly phenotypes/symptoms, not diseases)
        used_mondo = matched_mondo["nodenorm_MONDO"].dropna().unique()
        used_orph = matched_orph["nodenorm_orphanet"].dropna().unique()
        used_umls = matched_umls["nodenorm_UMLS"].dropna().unique()
        unmatched_nn = df_nn[
            ~df_nn["nodenorm_MONDO"].isin(used_mondo) &
            ~df_nn["nodenorm_orphanet"].isin(used_orph) &
            ~df_nn["nodenorm_UMLS"].isin(used_umls)
        ]
        if len(unmatched_nn):
            unmatched_nn.to_csv(self._qc_path("nodenorm_unmatched.csv"), index=False)
            logging.info(f"  ℹ️  {len(unmatched_nn):,} NodeNorm records unmatched "
                         f"(phenotypes/symptoms) — saved to QC, not merged")

        # Unmatched original rows (diseases that had no NodeNorm match)
        merged_ids = pd.concat([
            matched_mondo["mondo_id"], matched_orph["mondo_id"],
            matched_umls["mondo_id"]
        ], ignore_index=True).dropna().unique()
        unmatched_df = df_original[~df_original["mondo_id"].isin(merged_ids)].copy()
        unmatched_df["nodenorm_match_source"] = "unmatched_original"

        # Combine — matched + unmatched originals only (no unmatched NodeNorm)
        result = pd.concat([matched_mondo, matched_orph, matched_umls,
                            unmatched_df], ignore_index=True)
        result = result.loc[:, ~result.columns.str.endswith(("_x", "_y"))]

        logging.info(f"  → {result.shape[0]:,} rows after NodeNorm merge")
        del m1, m2, m3, matched_mondo, matched_orph, matched_umls
        del unmatched_nn, unmatched_df, df_original
        gc.collect()
        return result

    # ── reconcile orphaned rows ────────────────

    def _reconcile_pipe_orphans(self, df):
        """
        Fix orphaned rows from two causes:

        1. PIPE-DELIMITED MERGE KEY MISMATCHES: when mondo_omim = "OMIM:102730|OMIM:301083",
           pd.merge exact-match against omim_OMIM = "OMIM:301083" fails, leaving an orphan.

        2. NODENORM DUPLICATES: NodeNorm's cascaded merge can create a second row that
           shares an identifier (Orphanet ID, UMLS, etc.) with an existing main row but
           didn't merge because it matched on a different key path.

        For both: find orphaned rows (no mondo_id), try to match them to a main row
        via shared identifiers, fold their data in, drop the orphan.
        """
        before = len(df)

        # ── Phase 1: pipe-delimited merge key reconciliation ──
        # (pipe_col_in_main, source_id_col, source_prefix)
        pipe_pairs = [
            ("mondo_omim",     "omim_OMIM",           "omim_"),
            ("mondo_orphanet", "orphanet_Orphanet_ID", "orphanet_"),
        ]

        for pipe_col, src_id_col, src_prefix in pipe_pairs:
            if pipe_col not in df.columns or src_id_col not in df.columns:
                continue

            orphan_mask = df[src_id_col].notna() & df["mondo_id"].isna()
            orphans = df[orphan_mask]
            if len(orphans) == 0:
                continue

            logging.info(f"  🔗 Reconciling {len(orphans):,} orphaned {src_prefix} rows "
                         f"against pipe-delimited {pipe_col}...")

            # Reverse index: individual_id → main row indices
            main_mask = df[pipe_col].notna() & df["mondo_id"].notna()
            id_to_main = {}
            for idx in df.index[main_mask]:
                for single_id in split_pipe(df.at[idx, pipe_col]):
                    id_to_main.setdefault(single_id, []).append(idx)

            source_cols = [c for c in df.columns if c.startswith(src_prefix)]
            orphan_drops = []
            count = 0

            for orphan_idx, orphan_row in orphans.iterrows():
                oid = str(orphan_row[src_id_col]).strip()
                main_indices = id_to_main.get(oid, [])
                if not main_indices:
                    continue
                self._fold_row(df, main_indices[0], orphan_row, source_cols)
                orphan_drops.append(orphan_idx)
                count += 1

            if orphan_drops:
                df = df.drop(index=orphan_drops).reset_index(drop=True)
                logging.info(f"    ✅ Reconciled {count:,} pipe-orphans → {len(df):,} rows")

        # ── Phase 2: NodeNorm duplicate reconciliation ──
        # Find rows with no mondo_id that have a nodenorm identifier matching
        # a main row via shared cross-reference columns.
        orphan_mask = df["mondo_id"].isna() & df.filter(like="nodenorm_").notna().any(axis=1)
        orphans = df[orphan_mask]

        if len(orphans) > 0:
            logging.info(f"  🔗 Reconciling {len(orphans):,} mondo_id-less rows "
                         f"via shared identifiers...")

            # Match columns: if orphan has orphanet_X or medgen_X or umls_X,
            # find a main row with the same value
            match_cols = [
                ("orphanet_Orphanet_ID", "orphanet_Orphanet_ID"),
                ("nodenorm_orphanet",    "orphanet_Orphanet_ID"),
                ("nodenorm_orphanet",    "mondo_orphanet"),
                ("nodenorm_UMLS",        "medgen_UMLS"),
                ("nodenorm_UMLS",        "mondo_umls"),
                ("nodenorm_MONDO",       "mondo_id"),
            ]

            # Build lookup indices for main rows
            main_mask = df["mondo_id"].notna()
            main_df = df[main_mask]
            lookups = {}
            for _, main_col in match_cols:
                if main_col in df.columns and main_col not in lookups:
                    idx_map = {}
                    for idx in main_df.index:
                        val = main_df.at[idx, main_col]
                        if pd.notna(val):
                            for v in split_pipe(val):
                                idx_map.setdefault(v, []).append(idx)
                    lookups[main_col] = idx_map

            nodenorm_cols = [c for c in df.columns if c.startswith("nodenorm_")]
            orphan_drops = []
            count = 0

            for orphan_idx, orphan_row in orphans.iterrows():
                matched_main_idx = None

                for orphan_col, main_col in match_cols:
                    if orphan_col not in df.columns or main_col not in lookups:
                        continue
                    oval = orphan_row.get(orphan_col)
                    if pd.isna(oval):
                        continue
                    for v in split_pipe(oval):
                        candidates = lookups[main_col].get(v, [])
                        if candidates:
                            matched_main_idx = candidates[0]
                            break
                    if matched_main_idx is not None:
                        break

                if matched_main_idx is None:
                    continue

                # Fold all nodenorm columns (and any other populated non-label cols)
                fold_cols = nodenorm_cols
                self._fold_row(df, matched_main_idx, orphan_row, fold_cols)
                orphan_drops.append(orphan_idx)
                count += 1

            if orphan_drops:
                df = df.drop(index=orphan_drops).reset_index(drop=True)
                logging.info(f"    ✅ Reconciled {count:,} nodenorm duplicates → {len(df):,} rows")

        total_reconciled = before - len(df)
        if total_reconciled > 0:
            logging.info(f"  📊 Total reconciled: {total_reconciled:,} orphan rows removed")
        return df

    def _fold_row(self, df, target_idx, orphan_row, cols):
        """Fold orphan_row's values into df[target_idx] for the given columns."""
        for col in cols:
            if col not in df.columns:
                continue
            orphan_val = orphan_row.get(col)
            if pd.isna(orphan_val):
                continue
            existing = df.at[target_idx, col]
            if pd.isna(existing):
                df.at[target_idx, col] = orphan_val
            else:
                # Union pipe-delimited values
                merged = split_pipe(existing) | split_pipe(orphan_val)
                df.at[target_idx, col] = "|".join(sorted(merged))

    # ── consolidate duplicates ────────────────

    def consolidate_duplicate_mondo(self, df):
        """Merge rows sharing the same mondo_id (outer join artifacts)."""
        key = "mondo_id"
        if key not in df.columns:
            return df

        counts = df[key].value_counts()
        dupes = counts[counts > 1].index.tolist()
        if not dupes:
            logging.info("✅ No duplicate mondo_ids")
            return df

        logging.info(f"🔄 Consolidating {len(dupes):,} duplicated mondo_ids...")
        agg_dict = {col: concat_unique for col in df.columns if col != key}
        grouped = (df[df[key].isin(dupes)].groupby(key, dropna=False)
                   .agg(agg_dict).reset_index())
        singles = df[~df[key].isin(dupes)]
        result = pd.concat([singles, grouped], ignore_index=True)
        logging.info(f"  → {result.shape[0]:,} rows after consolidation")
        return result

    # ── harmonize and score (single pass) ─────

    def harmonize(self, df):
        """
        Single-pass harmonization that produces:
          - The harmonization DataFrame (for Excel output)
          - Adds consolidated/provenance/score columns to the master df
          - Detects obsolete IDs and flags rows where they drive mismatches
        """
        logging.info("🧮 Building harmonization output...")
        harm_df = build_harmonization_output(df, ID_GROUPS, self.obsolete_sets)

        # Log obsolete detection summary
        for canonical, _ in ID_GROUPS:
            flag_col = f"{canonical}_obsolete_flag"
            if flag_col in harm_df.columns:
                vc = harm_df[flag_col].value_counts(dropna=True)
                if not vc.empty:
                    parts = [f"{v}={c}" for v, c in vc.items()]
                    logging.info(f"    {canonical} obsolete flags: {', '.join(parts)}")

        return harm_df

    # ── column ordering ──────────────────────

    def reorder_master(self, df):
        """Order columns: source xrefs grouped by namespace, then labels, then extras."""
        ordered = []

        for canonical, pairs in ID_GROUPS:
            # Raw source columns
            for _, col in pairs:
                if col in df.columns and col not in ordered:
                    ordered.append(col)

        # Label/description columns
        label_like = [c for c in df.columns
                      if any(x in c.lower() for x in ["preferred", "definition", "synonym", "disease_name"])
                      and c not in ordered]
        ordered.extend(label_like)

        # Match source and metadata
        meta = [c for c in df.columns
                if c.startswith("nodenorm_match") or c in ("NodeNorm_id", "biolinkType")
                and c not in ordered]
        ordered.extend(meta)

        # Everything else
        extras = [c for c in df.columns if c not in ordered]
        ordered.extend(extras)

        return df[[c for c in ordered if c in df.columns]]

    # ── QC output generation ─────────────────

    def write_qc_files(self, df, harm_df):
        """Generate all focused QC files."""
        logging.info("📋 Generating QC files...")

        # Count obsolete-resolved rows (excluded from mismatch/cardinality QC)
        obs_resolved = 0
        for canonical, _ in ID_GROUPS:
            flag_col = f"{canonical}_obsolete_flag"
            if flag_col in harm_df.columns:
                obs_resolved += int((harm_df[flag_col] == "resolved").sum())
        if obs_resolved:
            logging.info(f"  🗑  {obs_resolved:,} flags resolved by obsolete ID removal "
                         f"(excluded from QC lists)")

        # 1. Mismatches — focused, human-readable (derived from harm_df)
        mismatch_df = build_mismatch_qc(harm_df, ID_GROUPS, LABEL_COLS)
        if not mismatch_df.empty:
            path = self._qc_path("mismatches.csv")
            mismatch_df.to_csv(path, index=False)
            logging.info(f"  ⚠️  {len(mismatch_df):,} mismatch rows → mismatches.csv")
        else:
            logging.info("  ✅ No mismatches")

        # 2. Cardinality — actual ID pairs (derived from harm_df)
        card_df = build_cardinality_qc(harm_df, ID_GROUPS, LABEL_COLS)
        if not card_df.empty:
            path = self._qc_path("cardinality.csv")
            card_df.to_csv(path, index=False)
            logging.info(f"  ⚠️  {len(card_df):,} cardinality issues → cardinality.csv")
        else:
            logging.info("  ✅ No cardinality issues")

        # 3. Duplicate mondo_ids (from raw df, before harmonization)
        dup_df = build_duplicate_mondo_qc(df)
        if not dup_df.empty:
            path = self._qc_path("duplicate_mondo.csv")
            dup_df.to_csv(path, index=False)
            logging.info(f"  ⚠️  {len(dup_df):,} rows with duplicate mondo_ids → duplicate_mondo.csv")

        # 4. Summary statistics
        summary_df = build_summary_qc(harm_df, ID_GROUPS)
        if not summary_df.empty:
            path = self._qc_path("summary.csv")
            summary_df.to_csv(path, index=False)
            logging.info(f"  📊 Summary → summary.csv")

    # ── Excel output ─────────────────────────

    def save_harmonization_excel(self, harm_df):
        """
        Harmonization Excel workbook — single sheet with the full harmonization table.
        QC files (mismatches, cardinality, summary) are separate CSVs only.

        Uses xlsxwriter for speed (290K+ rows).
        """
        try:
            import xlsxwriter
        except ImportError:
            logging.warning("xlsxwriter not installed — skipping Excel output. "
                            "Install with: pip install xlsxwriter")
            return

        output_path = self.config.get(
            "harmonization_excel_output",
            "src/data/publicdata/disease_data/harmonized/disease_harmonization.xlsx")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logging.info("📊 Building harmonization Excel...")

        # Put overall_quality first
        cols = ["overall_quality"] + [c for c in harm_df.columns if c != "overall_quality"]
        harm_df = harm_df[cols]

        wb = xlsxwriter.Workbook(output_path, {'strings_to_numbers': False,
                                                 'constant_memory': True})

        logging.info("  Writing Harmonization sheet...")
        self._write_harmonization_sheet_xlsxwriter(wb, harm_df)

        wb.close()
        logging.info(f"✅ Harmonization Excel → {output_path}")

    def _write_df_sheet_xlsxwriter(self, wb, sheet_name, df, empty_msg):
        """Write a small QC DataFrame with header styling."""
        ws = wb.add_worksheet(sheet_name)

        if df.empty:
            ws.write(0, 0, empty_msg)
            return

        header_fmt = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#1F3864',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
        })

        # Write headers
        for col_idx, col_name in enumerate(df.columns):
            ws.write(0, col_idx, col_name, header_fmt)

        # Write data
        for row_idx, row in enumerate(df.itertuples(index=False), 1):
            for col_idx, val in enumerate(row):
                if pd.notna(val):
                    ws.write(row_idx, col_idx, val)

        # Column widths
        for col_idx, col_name in enumerate(df.columns):
            max_len = max(len(str(col_name)),
                          df.iloc[:, col_idx].dropna().astype(str).str.len().max()
                          if len(df) > 0 else 0)
            ws.set_column(col_idx, col_idx, min(max_len + 2, 40))

        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(df), len(df.columns) - 1)

    def _write_harmonization_sheet_xlsxwriter(self, wb, harm_df):
        """
        Write the main harmonization sheet using xlsxwriter.
        Uses column-level formatting + conditional formatting rules
        instead of per-cell styling for massive speed improvement.
        """
        ws = wb.add_worksheet("Harmonization")
        n_rows = len(harm_df)
        n_cols = len(harm_df.columns)
        logging.info(f"    {n_rows:,} rows × {n_cols} cols")

        # ── Pre-build format objects ──
        # Headers by column type
        header_default = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#404040',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_overall = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#375623',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_agreement = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#2E75B6',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_quality = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#1F3864',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_value = wb.add_format({
            'bold': True, 'bg_color': '#D9E1F2',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_sources = wb.add_format({
            'bold': True, 'bg_color': '#E2EFDA',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_cardinality = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#7030A0',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })
        header_obsolete = wb.add_format({
            'bold': True, 'font_color': 'white', 'bg_color': '#BF8F00',
            'font_size': 9, 'font_name': 'Arial', 'text_wrap': True,
            'align': 'center', 'valign': 'vcenter',
            'border': 1, 'border_color': '#CCCCCC',
        })

        # Data formats (reusable — no per-cell object creation)
        data_left = wb.add_format({'font_size': 9, 'font_name': 'Arial'})
        data_center = wb.add_format({'font_size': 9, 'font_name': 'Arial',
                                      'align': 'center'})

        # Conditional formatting color formats
        agree_green = wb.add_format({'bg_color': '#C6EFCE', 'font_size': 9,
                                      'font_name': 'Arial', 'align': 'center'})
        agree_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_size': 9,
                                       'font_name': 'Arial', 'align': 'center'})
        agree_red = wb.add_format({'bg_color': '#FFC7CE', 'font_size': 9,
                                    'font_name': 'Arial', 'align': 'center'})
        agree_blue = wb.add_format({'bg_color': '#BDD7EE', 'font_size': 9,
                                     'font_name': 'Arial', 'align': 'center'})
        agree_grey = wb.add_format({'bg_color': '#F2F2F2', 'font_size': 9,
                                     'font_name': 'Arial', 'align': 'center'})
        agree_orange = wb.add_format({'bg_color': '#F4B084', 'font_size': 9,
                                       'font_name': 'Arial', 'align': 'center'})

        card_green = wb.add_format({'bg_color': '#C6EFCE', 'font_size': 9,
                                     'font_name': 'Arial', 'align': 'center'})
        card_yellow = wb.add_format({'bg_color': '#FFEB9C', 'font_size': 9,
                                      'font_name': 'Arial', 'align': 'center'})
        card_red = wb.add_format({'bg_color': '#FFC7CE', 'font_size': 9,
                                   'font_name': 'Arial', 'align': 'center'})
        card_darkred = wb.add_format({'bg_color': '#FF9999', 'font_size': 9,
                                       'font_name': 'Arial', 'align': 'center'})

        quality_high = wb.add_format({'bg_color': '#C6EFCE', 'font_size': 9,
                                       'font_name': 'Arial', 'align': 'center'})
        quality_mid = wb.add_format({'bg_color': '#FFEB9C', 'font_size': 9,
                                      'font_name': 'Arial', 'align': 'center'})
        quality_low = wb.add_format({'bg_color': '#BDD7EE', 'font_size': 9,
                                      'font_name': 'Arial', 'align': 'center'})
        quality_neg = wb.add_format({'bg_color': '#FFC7CE', 'font_size': 9,
                                      'font_name': 'Arial', 'align': 'center'})

        # ── Write headers ──
        ws.set_row(0, 40)
        for col_idx, col_name in enumerate(harm_df.columns):
            if col_name == "overall_quality":
                hdr = header_overall
            elif col_name.endswith("_agreement"):
                hdr = header_agreement
            elif col_name.endswith("_quality"):
                hdr = header_quality
            elif col_name.endswith("_value"):
                hdr = header_value
            elif col_name.endswith("_sources"):
                hdr = header_sources
            elif col_name.endswith("_cardinality"):
                hdr = header_cardinality
            elif col_name.endswith(("_obsolete_ids", "_obsolete_sources", "_obsolete_flag")):
                hdr = header_obsolete
            else:
                hdr = header_default
            ws.write(0, col_idx, col_name, hdr)

        # ── Write data rows (bulk, minimal formatting) ──
        logging.info(f"    Writing {n_rows:,} data rows...")
        for row_idx in range(n_rows):
            if (row_idx + 1) % 50000 == 0:
                logging.info(f"      {row_idx + 1:,} / {n_rows:,} rows written...")
            for col_idx in range(n_cols):
                val = harm_df.iat[row_idx, col_idx]
                if pd.isna(val):
                    continue  # skip empty cells entirely (faster)
                ws.write(row_idx + 1, col_idx, val)

        # ── Apply conditional formatting by column (batch, not per-cell) ──
        logging.info(f"    Applying conditional formatting...")
        last_data_row = n_rows  # row index n_rows = last data row (0-indexed header at 0)
        for col_idx, col_name in enumerate(harm_df.columns):

            if col_name.endswith("_agreement"):
                for val, fmt in [("agree", agree_green), ("subset", agree_orange),
                                 ("majority", agree_yellow),
                                 ("conflict", agree_red), ("single", agree_blue),
                                 ("no_data", agree_grey)]:
                    ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                        'type': 'text', 'criteria': 'containing',
                        'value': val, 'format': fmt,
                    })

            elif col_name.endswith("_cardinality"):
                for val, fmt in [("one_to_one", card_green), ("one_to_many", card_yellow),
                                 ("many_to_one", card_red), ("complex", card_darkred),
                                 ("no_data", agree_grey)]:
                    ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                        'type': 'text', 'criteria': 'containing',
                        'value': val, 'format': fmt,
                    })

            elif col_name.endswith("_quality"):
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '>=', 'value': 3,
                    'format': quality_high})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '==', 'value': 2,
                    'format': quality_mid})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '==', 'value': 1,
                    'format': quality_low})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '<', 'value': 0,
                    'format': quality_neg})

            elif col_name.endswith("_obsolete_flag"):
                obs_resolved = wb.add_format({'bg_color': '#FFF2CC', 'font_size': 9,
                                               'font_name': 'Arial', 'align': 'center',
                                               'font_color': '#BF8F00', 'bold': True})
                obs_contributes = wb.add_format({'bg_color': '#FCE4EC', 'font_size': 9,
                                                  'font_name': 'Arial', 'align': 'center'})
                obs_present = wb.add_format({'bg_color': '#F2F2F2', 'font_size': 9,
                                              'font_name': 'Arial', 'align': 'center'})
                for val, fmt in [("resolved", obs_resolved),
                                 ("contributes", obs_contributes),
                                 ("present", obs_present)]:
                    ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                        'type': 'text', 'criteria': 'containing',
                        'value': val, 'format': fmt,
                    })

            elif col_name == "overall_quality":
                oq_dark = wb.add_format({'bg_color': '#375623', 'font_color': 'white',
                                          'bold': True, 'font_size': 9, 'align': 'center'})
                oq_green = wb.add_format({'bg_color': '#70AD47', 'font_color': 'white',
                                           'bold': True, 'font_size': 9, 'align': 'center'})
                oq_yellow = wb.add_format({'bg_color': '#FFEB9C', 'bold': True,
                                            'font_size': 9, 'align': 'center'})
                oq_red = wb.add_format({'bg_color': '#FFC7CE', 'bold': True,
                                         'font_size': 9, 'align': 'center'})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '>=', 'value': 2.5, 'format': oq_dark})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '>=', 'value': 2.0, 'format': oq_green})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '>=', 'value': 1.0, 'format': oq_yellow})
                ws.conditional_format(1, col_idx, last_data_row, col_idx, {
                    'type': 'cell', 'criteria': '<', 'value': 1.0, 'format': oq_red})

        # ── Column widths (sample first 1000 rows for speed) ──
        sample_size = min(1000, n_rows)
        for col_idx, col_name in enumerate(harm_df.columns):
            sample_max = harm_df.iloc[:sample_size, col_idx].dropna().astype(str).str.len().max() \
                         if sample_size > 0 else 0
            width = max(len(col_name), sample_max if pd.notna(sample_max) else 0) + 2
            ws.set_column(col_idx, col_idx, min(width, 40))

        ws.freeze_panes(1, 1)
        ws.autofilter(0, 0, n_rows, n_cols - 1)

    # ── main entry point ─────────────────────

    def process(self):
        # 1. Merge all sources
        df = self.merge_sources()

        # 2. Consolidate duplicate mondo_ids (from outer joins)
        df = self.consolidate_duplicate_mondo(df)

        # 3. Reorder master columns
        df = self.reorder_master(df)

        # 4. Save master CSV
        output = self.config['merged_output']
        os.makedirs(os.path.dirname(output), exist_ok=True)
        df.to_csv(output, index=False)
        logging.info(f"✅ Master CSV → {output}  ({df.shape[0]:,} rows × {df.shape[1]} cols)")

        # 5. Build harmonization output
        harm_df = self.harmonize(df)

        # 6. Write focused QC files (CSVs)
        self.write_qc_files(df, harm_df)

        # 7. Save harmonization Excel + CSV
        self.save_harmonization_excel(harm_df)
        harm_csv = self.config.get(
            "harmonization_csv_output",
            self.config.get("harmonization_excel_output",
                            "src/data/publicdata/disease_data/harmonized/disease_harmonization.xlsx"
                            ).replace(".xlsx", ".csv"))
        os.makedirs(os.path.dirname(harm_csv), exist_ok=True)
        harm_df.to_csv(harm_csv, index=False)
        logging.info(f"✅ Harmonization CSV → {harm_csv}")

        # 8. Save clean xref file (high-quality agreed rows only)
        self.save_clean_xref(harm_df)

        # 9. Generate UpSet plots
        self.save_upset_plots(harm_df)

        logging.info("🏁 Pipeline complete.")

    # ── clean xref output ────────────────────

    def save_clean_xref(self, harm_df):
        """
        Produce a finalized, high-quality cross-reference file containing only
        rows where every populated namespace is 'agree' or 'single' (no conflicts,
        no ambiguity).

        Columns:
          - standard_id     : best disease identifier (mondo > medgen > first available)
          - standard_name   : preferred label from the standard_id source
          - {namespace}_value columns (the consensus/union values)
          - overall_quality
        """
        logging.info("📋 Building clean xref file...")

        # Filter: keep rows where ALL populated namespaces are agree, single, or subset
        # (i.e. no conflict, no majority with real disagreement)
        agr_cols = [f"{canonical}_agreement" for canonical, _ in ID_GROUPS
                    if f"{canonical}_agreement" in harm_df.columns]

        good_labels = {"agree", "single", "subset", "no_data"}

        def row_is_clean(row):
            for col in agr_cols:
                val = row.get(col)
                if pd.notna(val) and val not in good_labels:
                    return False
            return True

        mask = harm_df.apply(row_is_clean, axis=1)
        clean = harm_df[mask].copy()
        logging.info(f"  {mask.sum():,} / {len(harm_df):,} rows pass quality filter")

        # Build standard_id: mondo_value > medgen_value > first non-null _value
        value_cols = [f"{canonical}_value" for canonical, _ in ID_GROUPS
                      if f"{canonical}_value" in clean.columns]

        def pick_standard_id(row):
            # Priority: mondo, then medgen, then first available
            for col in ["mondo_value", "medgen_value"]:
                if col in clean.columns:
                    v = row.get(col)
                    if pd.notna(v) and "|" not in str(v):  # skip pipe-delimited (ambiguous)
                        return str(v).strip()
            # Fallback: first non-null, non-pipe value
            for col in value_cols:
                v = row.get(col)
                if pd.notna(v) and "|" not in str(v):
                    return str(v).strip()
            # Last resort: take first value even if pipe-delimited
            for col in value_cols:
                v = row.get(col)
                if pd.notna(v):
                    return str(v).split("|")[0].strip()
            return np.nan

        clean["standard_id"] = clean.apply(pick_standard_id, axis=1)

        # Build standard_name: prefer mondo label, then medgen, then doid, etc.
        name_priority = [
            "mondo_preferred_label", "medgen_Preferred_Name",
            "doid_preferred_label", "orphanet_Disease_Name",
            "omim_preferred_label", "nodenorm_Nodenorm_name",
        ]

        def pick_standard_name(row):
            for col in name_priority:
                if col in clean.columns:
                    v = row.get(col)
                    if pd.notna(v) and str(v).strip():
                        return str(v).strip()
            return np.nan

        clean["standard_name"] = clean.apply(pick_standard_name, axis=1)

        # Select final columns
        out_cols = ["standard_id", "standard_name", "overall_quality"]
        out_cols += [c for c in value_cols if c in clean.columns]
        clean = clean[[c for c in out_cols if c in clean.columns]]

        # Drop rows with no standard_id
        clean = clean[clean["standard_id"].notna()].copy()

        # Sort by overall_quality descending
        clean = clean.sort_values("overall_quality", ascending=False)

        output_path = self.config.get(
            "clean_xref_output",
            "src/data/publicdata/disease_data/harmonized/disease_xref_clean.csv")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        clean.to_csv(output_path, index=False)
        logging.info(f"✅ Clean xref → {output_path}  "
                     f"({len(clean):,} rows × {len(clean.columns)} cols)")

    # ── UpSet plots ──────────────────────────

    def save_upset_plots(self, harm_df):
        """Generate UpSet plots for source coverage and xref agreement."""
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            from upsetplot import UpSet
        except ImportError:
            logging.warning("upsetplot/matplotlib not installed — skipping UpSet plots. "
                            "Install with: pip install upsetplot matplotlib")
            return

        plot_dir = self.config.get(
            "plot_dir", "src/data/publicdata/disease_data/plots")
        os.makedirs(plot_dir, exist_ok=True)

        logging.info("📊 Generating UpSet plots...")

        # ── Source coverage UpSet ──
        # For each row, which sources have data present?
        SOURCE_INDICATORS = {
            "MedGen":   ["medgen_medgen", "medgen_mondo"],
            "MONDO":    ["mondo_mondo"],
            "Orphanet": ["orphanet_orphanet"],
            "DOID":     ["doid_DOID"],
            "OMIM":     ["omim_OMIM"],
            "NodeNorm": ["nodenorm_mondo"],
        }
        SOURCES_ORDER = ["MedGen", "MONDO", "Orphanet", "DOID", "OMIM", "NodeNorm"]

        presence = pd.DataFrame(index=harm_df.index)
        for source in SOURCES_ORDER:
            indicators = SOURCE_INDICATORS[source]
            mask = pd.Series(False, index=harm_df.index)
            for col in indicators:
                if col in harm_df.columns:
                    mask |= harm_df[col].notna() & (harm_df[col].astype(str).str.strip() != "")
            presence[source] = mask

        mi = pd.MultiIndex.from_frame(presence)
        counts = mi.value_counts()
        counts = counts[counts >= 100]
        counts = counts.sort_values(ascending=False)

        n_total = len(harm_df)

        fig = plt.figure(figsize=(20, 8))
        upset = UpSet(
            counts,
            subset_size="auto",
            show_counts=True,
            show_percentages=False,
            sort_by="cardinality",
            sort_categories_by="-input",
            min_subset_size=100,
            facecolor="black",
            element_size=36,
        )
        axes = upset.plot(fig=fig)
        fig.suptitle(
            f"Source Coverage Patterns Across {n_total:,} Disease Entities",
            fontsize=16, fontweight="bold", y=0.98,
        )
        plt.tight_layout()
        source_path = os.path.join(plot_dir, "disease_upset_source_coverage.png")
        fig.savefig(source_path, dpi=150, bbox_inches="tight",
                    facecolor="white", edgecolor="none")
        plt.close(fig)
        logging.info(f"  ✅ Source coverage UpSet → {source_path}")

        # Print source coverage summary
        for src in SOURCES_ORDER:
            n = presence[src].sum()
            logging.info(f"    {src:>10s}: {n:>8,}  ({n/n_total*100:.1f}%)")

        # ── Xref agreement UpSet ──
        # For each row, which namespaces have 'agree' or 'subset'?
        NAMESPACES = [
            "mondo", "DOID", "EFO", "GARD", "HP", "ICD10", "ICD9", "ICD11",
            "ICD11f", "ICDO", "KEGG", "MESH", "MEDDRA", "NCIT", "OMIM",
            "OMIMPS", "SNOMEDCT", "UMLS", "orphanet", "medgen",
        ]

        agree_presence = pd.DataFrame(index=harm_df.index)
        for ns in NAMESPACES:
            agr_col = f"{ns}_agreement"
            if agr_col in harm_df.columns:
                agree_presence[ns] = harm_df[agr_col].isin(["agree", "subset"])
            else:
                agree_presence[ns] = False

        # Keep namespaces with meaningful agreement counts
        active_ns = [ns for ns in NAMESPACES if agree_presence[ns].sum() > 100]
        agree_presence = agree_presence[active_ns]

        mi2 = pd.MultiIndex.from_frame(agree_presence)
        counts2 = mi2.value_counts()
        counts2 = counts2[counts2 >= 100]
        counts2 = counts2.sort_values(ascending=False)

        fig2 = plt.figure(figsize=(24, 10))
        upset2 = UpSet(
            counts2,
            subset_size="auto",
            show_counts=True,
            sort_by="cardinality",
            sort_categories_by="-input",
            min_subset_size=100,
            facecolor="black",
            element_size=32,
        )
        axes2 = upset2.plot(fig=fig2)
        fig2.suptitle(
            f"Xref Agreement Patterns — Namespaces with 'agree' or 'subset' "
            f"({n_total:,} diseases)",
            fontsize=14, fontweight="bold", y=0.98,
        )
        plt.tight_layout()
        xref_path = os.path.join(plot_dir, "disease_upset_xref_agreement.png")
        fig2.savefig(xref_path, dpi=150, bbox_inches="tight",
                     facecolor="white", edgecolor="none")
        plt.close(fig2)
        logging.info(f"  ✅ Xref agreement UpSet → {xref_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ODIN Disease Harmonization Pipeline")
    parser.add_argument("--config", required=True,
                        help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    setup_logging(config["disease_merge"].get("log_file"))
    DiseaseDataMerger(config).process()