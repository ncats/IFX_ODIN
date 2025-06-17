#!/usr/bin/env python
"""
hgnc_transform.py - Transform and clean HGNC data

This script reads the raw HGNC file (downloaded by hgnc_download.py),
applies the original cleaning steps (column subset, explode, type casts, etc.),
and writes the cleaned CSV along with detailed metadata.
"""

import os
import sys
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class HGNCTransformer:
    def __init__(self, full_config):
        self.config        = full_config.get("hgnc_data", {})
        self.input_file    = self.config.get("output_path", "hgnc_complete_set.txt")
        self.output_file   = self.config.get("parsed_output", "hgnc_complete_set.csv")
        self.metadata_file = self.config.get("tf_metadata_file", "tf_hgnc_metadata.json")

    def transform_and_clean_hgnc_data(self, df: pd.DataFrame):
        start_time = datetime.now()
        steps = []

        # 1) Trim whitespace from column names
        orig_cols = df.columns.tolist()
        df.columns = [c.strip() for c in orig_cols]
        steps.append(f"Trimmed whitespace from columns: {orig_cols} → {df.columns.tolist()}")

        # 2) Select and reorder the original set of columns
        cols = [
            'uniprot_ids', 'hgnc_id', 'symbol', 'entrez_id', 'ensembl_gene_id', 'name',
            'locus_group', 'locus_type', 'status', 'location', 'location_sortable',
            'alias_symbol', 'alias_name', 'prev_symbol', 'prev_name', 'gene_group',
            'gene_group_id', 'date_approved_reserved', 'date_symbol_changed',
            'date_name_changed', 'date_modified', 'vega_id', 'refseq_accession',
            'ccds_id', 'pubmed_id', 'omim_id', 'orphanet'
        ]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise KeyError(f"Missing required columns: {missing}")
        df = df[cols].copy()
        steps.append(f"Selected and copied columns: {cols}")

        # 3) Rename 'entrez_id' → 'hgnc_NCBI_id'
        df = df.rename(columns={'entrez_id': 'hgnc_NCBI_id'})
        steps.append("Renamed column 'entrez_id' → 'hgnc_NCBI_id'")

        # 4) Split and explode the pipe-separated uniprot_ids safely
        df.loc[:, 'uniprot_ids'] = df['uniprot_ids'].str.split('|')
        steps.append("Split 'uniprot_ids' on '|'")
        df = df.explode('uniprot_ids').reset_index(drop=True)
        steps.append("Exploded 'uniprot_ids' list into rows")

        # 5) Ensure those two ID columns are strings
        df['uniprot_ids']    = df['uniprot_ids'].astype(str)
        df['hgnc_NCBI_id']    = df['hgnc_NCBI_id' ].astype(str)

        # 6) Cast any float64 → string before fillna
        float_cols = df.select_dtypes(include=['float64']).columns.tolist()
        for c in float_cols:
            df[c] = df[c].astype(str)
        steps.append(f"Casted float64 columns to str: {float_cols}")

        # 7) Fill NaN → '' and clean orphanet
        df.fillna('', inplace=True)
        df['orphanet'] = df['orphanet'].replace('nan', '')
        steps.append("Filled NA with '' and cleaned 'orphanet'")

        # 8) Rename everything to hgnc_*
        rename_map = {
            'uniprot_ids': 'hgnc_uniprot_ids',
            'hgnc_id': 'hgnc_hgnc_id',
            'symbol': 'hgnc_symbol',
            'ensembl_gene_id': 'hgnc_ensembl_gene_id',
            'name': 'hgnc_description',
            'locus_group': 'hgnc_locus_group',
            'locus_type': 'hgnc_gene_type',
            'status': 'hgnc_status',
            'location': 'hgnc_location',
            'location_sortable': 'hgnc_location_sortable',
            'alias_symbol': 'hgnc_synonyms',
            'alias_name': 'hgnc_alias_name',
            'prev_symbol': 'hgnc_prev_symbol',
            'prev_name': 'hgnc_prev_name',
            'gene_group': 'hgnc_gene_group',
            'gene_group_id': 'hgnc_gene_group_id',
            'date_approved_reserved': 'hgnc_date_approved_reserved',
            'date_symbol_changed': 'hgnc_date_symbol_changed',
            'date_name_changed': 'hgnc_date_name_changed',
            'date_modified': 'hgnc_date_modified',
            'vega_id': 'hgnc_vega_id',
            'refseq_accession': 'hgnc_refseq_accession',
            'ccds_id': 'hgnc_ccds_id',
            'pubmed_id': 'hgnc_pubmed_id',
            'omim_id': 'hgnc_omim_id',
            'orphanet': 'hgnc_orphanet_id'
        }
        df = df.rename(columns=rename_map)
        steps.append(f"Renamed columns to include 'hgnc_' prefix")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        steps.append(f"Total transformation duration: {duration:.2f}s")

        return df, start_time, end_time, steps

    def run(self):
        # 1) Read input
        try:
            df = pd.read_csv(self.input_file, sep="\t", dtype=str, low_memory=False)
            num_in = len(df)
            logging.info(f"Read {num_in} records from {self.input_file}")
        except Exception as e:
            logging.error(f"Failed to read {self.input_file}: {e}")
            sys.exit(1)

        # 2) Transform & clean
        try:
            cleaned_df, t0, t1, steps = self.transform_and_clean_hgnc_data(df)
            num_out = len(cleaned_df)
            logging.info(f"Cleaned to {num_out} records")
        except Exception as e:
            logging.error(f"Transformation error: {e}")
            sys.exit(1)

        # 3) Write output CSV
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        cleaned_df.to_csv(self.output_file, index=False)
        logging.info(f"Wrote cleaned HGNC data to {self.output_file}")

        # 4) Write metadata JSON
        metadata = {
            "timestamp": {"start": t0.isoformat(), "end": t1.isoformat()},
            "input_file": self.input_file,
            "output_file": self.output_file,
            "num_records_input": num_in,
            "num_records_output": num_out,
            "transformation_duration_seconds": (t1 - t0).total_seconds(),
            "processing_steps": steps
        }
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w") as mf:
            json.dump(metadata, mf, indent=2)
        logging.info(f"HGNC transform metadata saved to {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform and clean HGNC data")
    parser.add_argument("--config", type=str, required=True,
                        help="Path to YAML config file")
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    HGNCTransformer(cfg).run()
