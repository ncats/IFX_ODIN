"""
Clean and normalize RDIP GARD rare disease file for the ODIN disease merge pipeline.

Input:  RDIP_RareDisease_v*.csv (from RDIP team)
        medgen_id_mappings.csv  (from MedGen cleaned source — for UMLS→MedGen ID lookup)
Output: gard_cleaned.csv with standardized column names prefixed with 'gard_'

Filters to active (non-deprecated) records only.
Normalizes ID formats to match other sources.
Maps UMLS CUIs to real MedGen IDs (RDIP's "equivalent_medgen" is just MEDGEN:+CUI, not actual MedGen IDs).
"""

import argparse
import pandas as pd
import numpy as np
import logging

def clean_gard(input_path, output_path, medgen_path=None):
    logging.info(f"Loading GARD file: {input_path}")
    df = pd.read_csv(input_path, dtype=str)
    logging.info(f"  Raw: {len(df):,} records")

    # Filter to active only
    df = df[df['deprecated'] == 'false'].copy()
    logging.info(f"  Active (non-deprecated): {len(df):,}")

    # Rename columns with gard_ prefix for merge pipeline
    out = pd.DataFrame()

    # Normalize GARD IDs: RDIP zero-pads to 7 digits (GARD:0015029)
    # but every other source uses unpadded (GARD:15029). Strip leading zeros.
    out['gard_GARD'] = df['id'].str.strip().str.replace(
        r'^GARD:0*(\d+)$', r'GARD:\1', regex=True)

    out['gard_name'] = df['name'].str.strip()
    out['gard_type'] = df['type'].str.strip()
    out['gard_MONDO'] = df['equivalent_mondo'].str.strip()

    # NOTE: RDIP's equivalent_umls is a bare CUI (e.g. "C1864002")
    # and equivalent_medgen is just "MEDGEN:" + that same CUI — fully redundant.
    # We store the bare CUI as gard_UMLS, then look up the REAL MedGen ID.
    umls_raw = df['equivalent_umls'].str.strip()
    out['gard_UMLS'] = umls_raw  # bare CUI like "C1864002"

    # Map UMLS CUI → real MedGen ID (e.g. C1864002 → MEDGEN:400240)
    if medgen_path:
        logging.info(f"  Loading MedGen for UMLS→MedGen ID mapping: {medgen_path}")
        mg = pd.read_csv(medgen_path, dtype=str, usecols=['medgen_UMLS', 'medgen_MedGen'])
        mg = mg.dropna(subset=['medgen_UMLS', 'medgen_MedGen'])
        mg = mg.drop_duplicates(subset='medgen_UMLS', keep='first')
        umls_to_medgen = dict(zip(mg['medgen_UMLS'].str.strip(), mg['medgen_MedGen'].str.strip()))
        logging.info(f"    {len(umls_to_medgen):,} UMLS→MedGen mappings loaded")

        out['gard_MEDGEN'] = umls_raw.map(umls_to_medgen)
        n_mapped = out['gard_MEDGEN'].notna().sum()
        n_unmapped = umls_raw.notna().sum() - n_mapped
        logging.info(f"    Mapped: {n_mapped:,} | Unmapped: {n_unmapped:,}")
    else:
        # Fallback: no MedGen file, just prefix the CUI
        logging.warning("  No MedGen file provided — gard_MEDGEN will be empty")
        out['gard_MEDGEN'] = np.nan

    # Keep provenance columns for traceability
    out['gard_created_source'] = df['created_source'].str.strip()
    out['gard_created_rule'] = df['created_rule'].str.strip()
    out['gard_evidence_source'] = df['evidence_source'].str.strip()

    # Clean: replace empty strings with NaN
    out = out.replace(r'^\s*$', np.nan, regex=True)

    logging.info(f"  Output: {len(out):,} rows × {len(out.columns)} cols")
    logging.info(f"  With Mondo: {out['gard_MONDO'].notna().sum():,}")
    logging.info(f"  With UMLS: {out['gard_UMLS'].notna().sum():,}")
    logging.info(f"  With MedGen ID: {out['gard_MEDGEN'].notna().sum():,}")
    logging.info(f"  Without Mondo: {out['gard_MONDO'].isna().sum():,}")

    out.to_csv(output_path, index=False)
    logging.info(f"  ✅ Saved → {output_path}")
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Clean RDIP GARD file for ODIN pipeline")
    parser.add_argument("--input", required=True, help="Path to RDIP CSV")
    parser.add_argument("--output", required=True, help="Output path for cleaned CSV")
    parser.add_argument("--medgen", default=None,
                        help="Path to medgen_id_mappings.csv (for UMLS→MedGen ID lookup)")
    args = parser.parse_args()
    clean_gard(args.input, args.output, medgen_path=args.medgen)