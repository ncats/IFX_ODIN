#!/usr/bin/env python
"""
hgnc_transform.py - Transform and clean HGNC data
with entity-level diff tracking, archive snapshots, and transform manifest metadata.
"""

import os
import sys
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime
from pathlib import Path
from publicdata.target_data.shared.output_versioning import save_versioned_output

from publicdata.target_data.download_utils import setup_logging

setup_logging()


class HGNCTransformer:
    def __init__(self, full_config):
        self.config = full_config.get("hgnc_data", {})

        self.input_file = self.config.get("output_path", "hgnc_complete_set.txt")
        self.output_file = self.config.get(
            "parsed_output",
            "src/data/publicdata/target_data/cleaned/sources/hgnc_complete_set.csv"
        )
        self.metadata_file = self.config.get(
            "tf_metadata_file",
            "src/data/publicdata/target_data/metadata/tf_hgnc_metadata.json"
        )
        self.entity_diff_file = self.config.get(
            "entity_diff_output",
            "src/data/publicdata/target_data/qc/hgnc_entity_diff.qc.json"
        )
        self.transform_archive_dir = self.config.get(
            "transform_archive_dir",
            "src/data/publicdata/target_data/archive/cleaned/hgnc"
        )

    def transform_and_clean_hgnc_data(self, df: pd.DataFrame):
        start_time = datetime.now()
        steps = []

        orig_cols = df.columns.tolist()
        df.columns = [c.strip() for c in orig_cols]
        steps.append(f"Trimmed whitespace from columns: {orig_cols} → {df.columns.tolist()}")

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

        df = df.rename(columns={'entrez_id': 'hgnc_NCBI_id'})
        steps.append("Renamed column 'entrez_id' → 'hgnc_NCBI_id'")

        df.loc[:, 'uniprot_ids'] = df['uniprot_ids'].str.split('|')
        steps.append("Split 'uniprot_ids' on '|'")
        df = df.explode('uniprot_ids').reset_index(drop=True)
        steps.append("Exploded 'uniprot_ids' list into rows")

        df['uniprot_ids'] = df['uniprot_ids'].astype(str)
        df['hgnc_NCBI_id'] = df['hgnc_NCBI_id'].astype(str)

        float_cols = df.select_dtypes(include=['float64']).columns.tolist()
        for c in float_cols:
            df[c] = df[c].astype(str)
        steps.append(f"Casted float64 columns to str: {float_cols}")

        df.fillna('', inplace=True)
        df['orphanet'] = df['orphanet'].replace('nan', '')
        steps.append("Filled NA with '' and cleaned 'orphanet'")

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
        steps.append("Renamed columns to include 'hgnc_' prefix")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        steps.append(f"Total transformation duration: {duration:.2f}s")

        return df, start_time, end_time, steps

    def compute_entity_diff(self, old_df, new_df):
        old_df = old_df.fillna("").copy()
        new_df = new_df.fillna("").copy()

        old_ids = set(old_df["hgnc_hgnc_id"])
        new_ids = set(new_df["hgnc_hgnc_id"])

        old_ids.discard("")
        new_ids.discard("")

        added = sorted(list(new_ids - old_ids))
        removed = sorted(list(old_ids - new_ids))

        old_idx = old_df.set_index("hgnc_hgnc_id")
        new_idx = new_df.set_index("hgnc_hgnc_id")

        common_ids = sorted(list(old_ids & new_ids))
        field_changes = []

        compare_cols = [
            "hgnc_symbol",
            "hgnc_ensembl_gene_id",
            "hgnc_uniprot_ids",
            "hgnc_NCBI_id",
            "hgnc_description",
            "hgnc_gene_type",
            "hgnc_status",
            "hgnc_omim_id",
            "hgnc_orphanet_id",
        ]

        for hgnc_id in common_ids:
            for col in compare_cols:
                if col not in old_idx.columns or col not in new_idx.columns:
                    continue

                old_val = str(old_idx.at[hgnc_id, col])
                new_val = str(new_idx.at[hgnc_id, col])

                if old_val != new_val:
                    field_changes.append({
                        "hgnc_hgnc_id": hgnc_id,
                        "field": col,
                        "old": old_val,
                        "new": new_val
                    })

        return {
            "added_ids": added,
            "removed_ids": removed,
            "field_changes": field_changes,
            "n_added_ids": len(added),
            "n_removed_ids": len(removed),
            "n_field_changes": len(field_changes),
        }

    def archive_output(self, cleaned_df):
        version = datetime.now().strftime("%Y%m%d")
        archive_dir = Path(self.transform_archive_dir) / version
        archive_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archive_dir / Path(self.output_file).name
        cleaned_df.to_csv(archive_path, index=False)
        return str(archive_path)

    def run(self):
        try:
            df = pd.read_csv(self.input_file, sep="\t", dtype=str, low_memory=False)
            num_in = len(df)
            logging.info(f"Read {num_in} records from {self.input_file}")
        except Exception as e:
            logging.error(f"Failed to read {self.input_file}: {e}")
            sys.exit(1)

        try:
            cleaned_df, t0, t1, steps = self.transform_and_clean_hgnc_data(df)
            num_out = len(cleaned_df)
            logging.info(f"Cleaned to {num_out} records")
        except Exception as e:
            logging.error(f"Transformation error: {e}")
            sys.exit(1)

        ver_result = save_versioned_output(
            df=cleaned_df,
            output_path=self.output_file,
            id_col="hgnc_hgnc_id",
            sep=",",
            write_diff=False,
            archive_dir=self.transform_archive_dir,
            output_kind="cleaned_source_table",
        )
        logging.info(f"Wrote cleaned HGNC data to {self.output_file}")

        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(self.output_file))[0]
        backup_path = os.path.join(qc_dir, f"{base}.backup.csv")

        diff_summary = None
        if os.path.exists(backup_path):
            try:
                old_df = pd.read_csv(backup_path, dtype=str)
                diff_summary = self.compute_entity_diff(old_df, cleaned_df)

                with open(self.entity_diff_file, "w") as f:
                    json.dump(diff_summary, f, indent=2)

                logging.info(f"Entity diff written to {self.entity_diff_file}")
            except Exception as e:
                logging.warning(f"⚠️ Could not generate entity diff on cleaned output: {e}")

        cleaned_df.to_csv(backup_path, index=False)

        archive_path = ver_result["archive_path"] or self.archive_output(cleaned_df)

        metadata = {
            "timestamp": {"start": t0.isoformat(), "end": t1.isoformat()},
            "input_file": self.input_file,
            "output_file": self.output_file,
            "archived_output": archive_path,
            "output_versioning": ver_result,
            "num_records_input": num_in,
            "num_records_output": num_out,
            "transformation_duration_seconds": (t1 - t0).total_seconds(),
            "processing_steps": steps,
            "summary": {
                "n_added_ids": diff_summary["n_added_ids"] if diff_summary else 0,
                "n_removed_ids": diff_summary["n_removed_ids"] if diff_summary else 0,
                "n_field_changes": diff_summary["n_field_changes"] if diff_summary else 0,
                "entity_diff_file": self.entity_diff_file if diff_summary else None,
            }
        }

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w") as mf:
            json.dump(metadata, mf, indent=2)

        logging.info(f"HGNC transform metadata saved to {self.metadata_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform and clean HGNC data")
    parser.add_argument(
        "--config",
        type=str,
        default="config/targets_config.yaml",
        help="Path to YAML config file"
    )

    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    HGNCTransformer(cfg).run()
