#!/usr/bin/env python
"""
ncbi_transform.py - Transform and clean NCBI gene_info data
with entity-level diff tracking, archive snapshots, and transform manifest metadata.
"""

import os
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)


class NCBITransformer:
    def __init__(self, full_config):
        self.config = full_config["ncbi_data"]

        self.input_file = self.config.get(
            "decompressed_file",
            "src/data/publicdata/target_data/raw/ncbi_gene_info.tsv"
        )

        self.output_file = self.config.get(
            "parsed_output",
            "src/data/publicdata/target_data/cleaned/sources/ncbi_gene_info.csv"
        )

        self.metadata_file = self.config.get(
            "tf_metadata_file",
            "src/data/publicdata/target_data/metadata/tf_ncbi_metadata.json"
        )

        self.entity_diff_file = self.config.get(
            "entity_diff_output",
            "src/data/publicdata/target_data/qc/ncbi_entity_diff.qc.json"
        )

        self.transform_archive_dir = self.config.get(
            "transform_archive_dir",
            "src/data/publicdata/target_data/archive/cleaned/ncbi"
        )

    def transform_and_clean_ncbi_data(self, ncbi_df):
        start_time = datetime.now()
        processing_steps = []

        ncbi_df.columns = [col.lstrip('#') for col in ncbi_df.columns]

        rename_mapping = {
            'GeneID': 'NCBI_id',
            'Symbol': 'symbol',
            'Synonyms': 'synonyms'
        }
        ncbi_df.rename(columns=rename_mapping, inplace=True)

        required_cols = [
            'NCBI_id', 'symbol', 'synonyms', 'dbXrefs', 'chromosome',
            'map_location', 'description', 'type_of_gene',
            'Modification_date', 'Feature_type'
        ]
        ncbi_df = ncbi_df[required_cols].copy()

        processing_steps.append({
            "step": "subset_required_columns",
            "records": len(ncbi_df)
        })

        ncbi_df['dbXrefs'] = ncbi_df['dbXrefs'].str.replace('HGNC:HGNC:', 'HGNC:', regex=False)
        ncbi_df['dbXrefs'] = ncbi_df['dbXrefs'].str.replace('AllianceGenome:HGNC:', 'AG:', regex=False)
        ncbi_df['dbXrefs'] = ncbi_df['dbXrefs'].str.split('|')

        exploded = ncbi_df.explode('dbXrefs')
        exploded.dropna(inplace=True)
        exploded['source'] = exploded['dbXrefs'].str.split(':').str[0]

        processing_steps.append({
            "step": "explode_dbxrefs",
            "records": len(exploded)
        })

        pivot = exploded.pivot_table(
            index='NCBI_id',
            columns='source',
            values='dbXrefs',
            aggfunc=lambda x: '|'.join(x)
        ).reset_index()

        pivot.rename(columns={
            "Ensembl": "ensembl_gene_id",
            "HGNC": "hgnc_id",
            "MIM": "mim_id",
            "miRBase": "miR_id",
            "IMGT/GENE-DB": "imgt_id"
        }, inplace=True)

        if 'ensembl_gene_id' in pivot.columns:
            pivot['ensembl_gene_id'] = pivot['ensembl_gene_id'].str.replace('Ensembl:', '', regex=False)

        merged = pd.merge(
            exploded[['NCBI_id', 'symbol', 'synonyms', 'chromosome',
                      'map_location', 'description', 'type_of_gene',
                      'Modification_date', 'Feature_type']],
            pivot,
            on='NCBI_id',
            how='right'
        ).drop_duplicates()

        if 'ensembl_gene_id' in merged.columns:
            merged['ensembl_gene_id'] = merged['ensembl_gene_id'].str.split('|')
            merged = merged.explode('ensembl_gene_id')

        rename_prefix = {
            'NCBI_id': 'ncbi_NCBI_id',
            'symbol': 'ncbi_symbol',
            'synonyms': 'ncbi_synonyms',
            'chromosome': 'ncbi_chromosome',
            'map_location': 'ncbi_location',
            'description': 'ncbi_description',
            'type_of_gene': 'ncbi_gene_type',
            'Modification_date': 'ncbi_Modification_date',
            'Feature_type': 'ncbi_Feature_type',
            'hgnc_id': 'ncbi_hgnc_id',
            'ensembl_gene_id': 'ncbi_ensembl_gene_id',
            'mim_id': 'ncbi_mim_id',
            'miR_id': 'ncbi_miR_id',
            'imgt_id': 'ncbi_imgt_id'
        }
        merged.rename(columns=rename_prefix, inplace=True)

        processing_steps.append({
            "step": "final_rename_and_expand",
            "records": len(merged)
        })

        end_time = datetime.now()
        return merged, start_time, end_time, processing_steps

    def compute_entity_diff(self, old_df, new_df):
        old_df = old_df.fillna("").copy()
        new_df = new_df.fillna("").copy()

        old_ids = set(old_df["ncbi_NCBI_id"])
        new_ids = set(new_df["ncbi_NCBI_id"])

        old_ids.discard("")
        new_ids.discard("")

        added = sorted(list(new_ids - old_ids))
        removed = sorted(list(old_ids - new_ids))

        old_idx = old_df.set_index("ncbi_NCBI_id")
        new_idx = new_df.set_index("ncbi_NCBI_id")

        common_ids = sorted(list(old_ids & new_ids))
        field_changes = []

        compare_cols = [
            "ncbi_symbol",
            "ncbi_ensembl_gene_id",
            "ncbi_hgnc_id",
            "ncbi_mim_id",
            "ncbi_synonyms",
            "ncbi_description",
            "ncbi_gene_type",
        ]

        for gene_id in common_ids:
            for col in compare_cols:
                if col not in old_idx.columns or col not in new_idx.columns:
                    continue

                old_val = str(old_idx.at[gene_id, col])
                new_val = str(new_idx.at[gene_id, col])

                if old_val != new_val:
                    field_changes.append({
                        "ncbi_NCBI_id": gene_id,
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

    def archive_output(self, transformed_df):
        version = datetime.now().strftime("%Y%m%d")
        archive_dir = Path(self.transform_archive_dir) / version
        archive_dir.mkdir(parents=True, exist_ok=True)

        archive_path = archive_dir / Path(self.output_file).name
        transformed_df.to_csv(archive_path, index=False)
        return str(archive_path)

    def run(self):
        ncbi_df = pd.read_csv(self.input_file, sep="\t", dtype=str)

        transformed_df, start_time, end_time, processing_steps = self.transform_and_clean_ncbi_data(ncbi_df)

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        transformed_df.to_csv(self.output_file, index=False)

        logging.info(f"Saved cleaned NCBI data → {self.output_file}")

        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)

        base = os.path.splitext(os.path.basename(self.output_file))[0]
        backup_path = os.path.join(qc_dir, f"{base}.backup.csv")

        diff_summary = None
        if os.path.exists(backup_path):
            old_df = pd.read_csv(backup_path, dtype=str)
            diff_summary = self.compute_entity_diff(old_df, transformed_df)

            with open(self.entity_diff_file, "w") as f:
                json.dump(diff_summary, f, indent=2)

            logging.info(f"Entity diff written → {self.entity_diff_file}")

        transformed_df.to_csv(backup_path, index=False)

        archive_path = self.archive_output(transformed_df)

        meta = {
            "timestamp": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            },
            "input_file": self.input_file,
            "output_file": self.output_file,
            "archived_output": archive_path,
            "processing_steps": processing_steps,
            "records_output": len(transformed_df),
            "summary": {
                "n_added_ids": diff_summary["n_added_ids"] if diff_summary else 0,
                "n_removed_ids": diff_summary["n_removed_ids"] if diff_summary else 0,
                "n_field_changes": diff_summary["n_field_changes"] if diff_summary else 0,
                "entity_diff_file": self.entity_diff_file if diff_summary else None,
            }
        }

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)

        logging.info("Metadata saved")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/targets_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    NCBITransformer(config).run()